// Looks up the two peers associated with a session so the signaling flow can
// route WebRTC negotiation messages between them.
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crate::workers::{self, SessionConnectionInfo, WorkerRegistry};
use crate::session::SessionRegistry;

pub async fn signaling(
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


pub fn websocket_connection(){
    
}