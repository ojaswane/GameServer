//! Opaque WebSocket relay for WebRTC signaling frames. The Control Panel uses
//! the session ID to route frames and does not inspect their contents.

use axum::{
    extract::{
        ConnectInfo, Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::{RwLock, mpsc};
use serde::{Deserialize, Serialize};
use crate::{session::SessionRegistry, workers::WorkerRegistry};

type Outbound = mpsc::Sender<Message>;

#[derive(Default)]
struct SessionConnections {
    worker: Option<Outbound>,
    client: Option<Outbound>,
}

// FOR SDP message 
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SignalingMessage {
    Offer {
        sdp: String,
    },
    Answer {
        sdp: String,
    },
    Candidate {
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u32>,
    },
}

#[derive(Default)]
struct Connections {
    sessions: HashMap<String, SessionConnections>,
}

type ConnectionRegistry = Arc<RwLock<Connections>>;

#[derive(Clone)]
pub struct SignalingState {
    workers: WorkerRegistry,
    sessions: SessionRegistry,
    connections: ConnectionRegistry,
}

impl SignalingState {
    pub fn new(workers: WorkerRegistry, sessions: SessionRegistry) -> Self {
        Self {
            workers,
            sessions,
            connections: Arc::new(RwLock::new(Connections::default())),
        }
    }
}

#[derive(Clone, Copy)]
enum PeerRole {
    Worker,
    Client,
}

impl PeerRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Client => "client",
        }
    }
}

/// A worker opens one signaling socket for each session it has claimed.
pub async fn worker_socket(
    Path((worker_id, session_id)): Path<(String, String)>,
    State(state): State<SignalingState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    if !state.workers.read().await.contains_key(&worker_id) {
        tracing::warn!(worker_id = %worker_id, "worker signaling connection rejected: worker not registered");
        return Err((StatusCode::NOT_FOUND, "Worker not found".to_string()));
    }

    let assigned = assigned_worker(&state.sessions, &session_id).await?;
    if assigned != worker_id {
        tracing::warn!(
            worker_id = %worker_id,
            session_id = %session_id,
            assigned_worker_id = %assigned,
            "worker signaling connection rejected: wrong worker assignment"
        );
        return Err((
            StatusCode::FORBIDDEN,
            "Worker is not assigned to this session".to_string(),
        ));
    }

    Ok(ws.on_upgrade(move |socket| {
        run_peer_socket(
            socket,
            session_id,
            worker_id,
            PeerRole::Worker,
            remote,
            state,
        )
    }))
}

/// A browser opens a signaling socket for its session after worker assignment.
pub async fn client_socket(
    Path(session_id): Path<String>,
    State(state): State<SignalingState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    let worker_id = assigned_worker(&state.sessions, &session_id).await?;
    let connections = state.connections.read().await;
    let worker_connected = connections
        .sessions
        .get(&session_id)
        .and_then(|peers| peers.worker.as_ref())
        .is_some();
    if !worker_connected {
        tracing::warn!(session_id = %session_id, "client signaling connection rejected: worker socket is not connected");
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Assigned worker is not connected to signaling".to_string(),
        ));
    }
    drop(connections);

    Ok(ws.on_upgrade(move |socket| {
        run_peer_socket(
            socket,
            session_id,
            worker_id,
            PeerRole::Client,
            remote,
            state,
        )
    }))
}

async fn assigned_worker(
    sessions: &SessionRegistry,
    session_id: &str,
) -> Result<String, (StatusCode, String)> {
    let sessions = sessions.read().await;
    let session = sessions
        .values()
        .find(|session| session.id == session_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Session not found".to_string()))?;

    session.worker_id.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "Session has not been assigned to a worker".to_string(),
        )
    })
}

async fn run_peer_socket(
    socket: WebSocket,
    session_id: String,
    worker_id: String,
    role: PeerRole,
    remote: SocketAddr,
    state: SignalingState,
) {
    let (outbound, mut outgoing) = mpsc::channel::<Message>(64);
    {
        let mut connections = state.connections.write().await;
        let peers = connections.sessions.entry(session_id.clone()).or_default();
        match role {
            PeerRole::Worker => peers.worker = Some(outbound.clone()),
            PeerRole::Client => peers.client = Some(outbound.clone()),
        }
    }

    tracing::info!(
        session_id = %session_id,
        worker_id = %worker_id,
        peer = role.as_str(),
        remote = %remote,
        "signaling WebSocket connected"
    );

    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(message @ Message::Text(_)))
                    | Some(Ok(message @ Message::Binary(_))) => {
                        // Forward application frames unchanged; only the session route is inspected.
                        let target = {
                            let connections = state.connections.read().await;
                            connections.sessions.get(&session_id).and_then(|peers| match role {
                                PeerRole::Worker => peers.client.clone(),
                                PeerRole::Client => peers.worker.clone(),
                            })
                        };

                        let Some(target) = target else {
                            tracing::warn!(
                                session_id = %session_id,
                                peer = role.as_str(),
                                "signaling frame received without a connected peer"
                            );
                            break;
                        };
                        if target.send(message).await.is_err() {
                            tracing::warn!(session_id = %session_id, peer = role.as_str(), "could not queue signaling frame for peer");
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if sink.send(Message::Pong(bytes)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(Message::Pong(_))) => {}
                }
            }
            outgoing_message = outgoing.recv() => {
                match outgoing_message {
                    Some(message) => {
                        if let Some((frame_type, bytes)) = frame_info(&message) {
                            let direction = match role {
                                PeerRole::Worker => "client_to_worker",
                                PeerRole::Client => "worker_to_client",
                            };
                            tracing::debug!(
                                session_id = %session_id,
                                direction,
                                frame_type,
                                bytes,
                                "signaling frame forwarded"
                            );
                        }
                        if sink.send(message).await.is_err() { break; }
                    }
                    None => break,
                }
            }
        }
    }

    let mut connections = state.connections.write().await;
    let mut peer_to_close = None;
    if let Some(peers) = connections.sessions.get_mut(&session_id) {
        let own_slot = match role {
            PeerRole::Worker => &mut peers.worker,
            PeerRole::Client => &mut peers.client,
        };
        if own_slot
            .as_ref()
            .is_some_and(|active| active.same_channel(&outbound))
        {
            *own_slot = None;
            peer_to_close = match role {
                PeerRole::Worker => peers.client.clone(),
                PeerRole::Client => peers.worker.clone(),
            };
        }
    }
    let remove_session = connections
        .sessions
        .get(&session_id)
        .is_some_and(|peers| peers.worker.is_none() && peers.client.is_none());
    if remove_session {
        connections.sessions.remove(&session_id);
    }
    drop(connections);

    if let Some(peer) = peer_to_close {
        let _ = peer.send(Message::Close(None)).await;
    }
    tracing::info!(
        session_id = %session_id,
        worker_id = %worker_id,
        peer = role.as_str(),
        "signaling WebSocket disconnected"
    );
}

fn frame_info(message: &Message) -> Option<(&'static str, usize)> {
    match message {
        Message::Text(text) => Some(("text", text.len())),
        Message::Binary(bytes) => Some(("binary", bytes.len())),
        _ => None,
    }
}
