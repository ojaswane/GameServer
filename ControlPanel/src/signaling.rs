// Looks up the two peers associated with a session so the signaling flow can
// route WebRTC negotiation messages between them.
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crate::workers::{self, SessionConnectionInfo, WorkerRegistry};
use crate::session::SessionRegistry;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message, WebSocketStream};

// Holds the active WebSocket sender channels
type Tx = SplitSink<WebSocketStream<TcpStream>, Message>;

pub async fn connections(
    Path(session_id): Path<String>,
    State((workers, sessions)): State<(WorkerRegistry, SessionRegistry)>,
) -> Result<Json<SessionConnectionInfo>, (StatusCode, String)> {
    let Json(info) = workers::session_connection_info(
        Path(session_id),
        State((workers, sessions)),
    )
    .await?;

    // These locals are available to use when routing offer/answer/ICE messages.
    let client_id = info.client_id;
    let client_ip = info.client_ip;
    let session_id = info.session_id;
    let worker_id = info.worker_id;
    let worker_ip = info.worker_ip;

    Ok(Json(SessionConnectionInfo {
        client_id,
        client_ip,
        session_id,
        worker_id,
        worker_ip,
    }))
}


pub async fn signaling(peers: PeerMap, stream: TcpStream, addr: SocketAddr){
    println!("New peer-to-peer connection {}", addr)

    let my_id = Arc::new(Mutex::new(String::new()));
    let my_id_cb = my_id.clone();

    // Accept the WebSocket handshake
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("Error during WebSocket handshake with {}: {}", addr, e);
            return;
        }
    };

    // clients and worker id's
    let Json(conn) = connections();

    let client_ip = conn.client_id;
    let worker_ip = conn.worker_id;


}