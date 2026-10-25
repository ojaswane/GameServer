//! WebSocket signaling relay. WebRTC offer/answer/ICE messages pass through
//! this module; audio and video media do not.

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, State},
    http::StatusCode,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, RwLock};

use crate::{
    session::SessionRegistry,
    workers::WorkerRegistry,
};

type Outbound = mpsc::Sender<String>;

#[derive(Default)]
struct Connections {
    workers: HashMap<String, Outbound>,
    clients: HashMap<String, Outbound>,
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

#[derive(Deserialize)]
struct WorkerEnvelope {
    session_id: String,
    message: Value,
}

/// Game agents connect once and receive signaling messages for their sessions.
pub async fn worker_socket(
    Path(worker_id): Path<String>,
    State(state): State<SignalingState>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    if !state.workers.read().await.contains_key(&worker_id) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Worker '{worker_id}' was not found"),
        ));
    }

    Ok(ws.on_upgrade(move |socket| run_worker_socket(socket, worker_id, state)))
}

/// A browser connects to the socket for its session after the worker claims it.
pub async fn client_socket(
    Path(session_id): Path<String>,
    State(state): State<SignalingState>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    let worker_id = assigned_worker(&state.sessions, &session_id).await?;
    let connections = state.connections.read().await;
    if !connections.workers.contains_key(&worker_id) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Assigned worker is not connected to signaling".to_string(),
        ));
    }
    drop(connections);

    Ok(ws.on_upgrade(move |socket| run_client_socket(socket, session_id, worker_id, state)))
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

async fn run_worker_socket(socket: WebSocket, worker_id: String, state: SignalingState) {
    let (outbound, mut outgoing) = mpsc::channel::<String>(64);
    {
        let mut connections = state.connections.write().await;
        connections.workers.insert(worker_id.clone(), outbound.clone());
    }

    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(error) = relay_worker_message(&worker_id, text.as_str(), &state).await {
                            let _ = outbound.send(json!({"type": "signaling_error", "error": error}).to_string()).await;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if sink.send(Message::Pong(bytes)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            message = outgoing.recv() => {
                match message {
                    Some(message) => {
                        if sink.send(Message::Text(message.into())).await.is_err() { break; }
                    }
                    None => break,
                }
            }
        }
    }

    let mut connections = state.connections.write().await;
    if connections
        .workers
        .get(&worker_id)
        .is_some_and(|active| active.same_channel(&outbound))
    {
        connections.workers.remove(&worker_id);
    }
}

async fn run_client_socket(
    socket: WebSocket,
    session_id: String,
    worker_id: String,
    state: SignalingState,
) {
    let (outbound, mut outgoing) = mpsc::channel::<String>(64);
    {
        let mut connections = state.connections.write().await;
        connections.clients.insert(session_id.clone(), outbound.clone());
    }

    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(payload) = serde_json::from_str::<Value>(text.as_str()) else {
                            let _ = outbound.send(json!({"type": "signaling_error", "error": "Message must be valid JSON"}).to_string()).await;
                            continue;
                        };
                        if !payload.is_object() {
                            let _ = outbound.send(json!({"type": "signaling_error", "error": "Message must be a JSON object"}).to_string()).await;
                            continue;
                        }

                        let worker_sender = state.connections.read().await.workers.get(&worker_id).cloned();
                        let Some(worker_sender) = worker_sender else {
                            let _ = outbound.send(json!({"type": "signaling_error", "error": "Assigned worker disconnected"}).to_string()).await;
                            continue;
                        };

                        let envelope = json!({"session_id": session_id, "message": payload}).to_string();
                        if worker_sender.send(envelope).await.is_err() {
                            let _ = outbound.send(json!({"type": "signaling_error", "error": "Assigned worker disconnected"}).to_string()).await;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if sink.send(Message::Pong(bytes)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            message = outgoing.recv() => {
                match message {
                    Some(message) => {
                        if sink.send(Message::Text(message.into())).await.is_err() { break; }
                    }
                    None => break,
                }
            }
        }
    }

    let mut connections = state.connections.write().await;
    if connections
        .clients
        .get(&session_id)
        .is_some_and(|active| active.same_channel(&outbound))
    {
        connections.clients.remove(&session_id);
    }
}

async fn relay_worker_message(
    worker_id: &str,
    text: &str,
    state: &SignalingState,
) -> Result<(), String> {
    let envelope: WorkerEnvelope = serde_json::from_str(text)
        .map_err(|_| "Worker message must contain session_id and message".to_string())?;
    if !envelope.message.is_object() {
        return Err("message must be a JSON object".to_string());
    }

    let assigned = assigned_worker(&state.sessions, &envelope.session_id)
        .await
        .map_err(|(_, message)| message)?;
    if assigned != worker_id {
        return Err("Worker is not assigned to this session".to_string());
    }

    let client_sender = state
        .connections
        .read()
        .await
        .clients
        .get(&envelope.session_id)
        .cloned()
        .ok_or_else(|| "Client is not connected to signaling".to_string())?;

    client_sender
        .send(envelope.message.to_string())
        .await
        .map_err(|_| "Client disconnected".to_string())
}
