//! Session data exchanged between the browser, control panel, and game agent.

use axum::{
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};

/// Lifecycle of a requested game session.
///
/// `Queued` and `Starting` are useful to distinguish waiting for an agent from
/// an agent actively preparing the game. `Ready` means the browser can connect.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Queued,
    Starting,
    Ready,
    Running,
    Closing,
    Closed,
    Failed,
}

/// Request accepted from a browser is in `ClientSession`

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionRequest {
    pub game_name: String,
}

/// The control panel's record for one browser game session.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSession {
    pub id: String,
    pub game_name: String,
    pub worker_id: Option<String>,
    pub status: SessionStatus,
    pub stream_url: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub failure_reason: Option<String>,
}

impl ClientSession {
    /// Make a new session record after accepting a browser request.
    pub fn queued(id: String, game_name: String, now_ms: u64) -> Self {
        Self {
            id,
            game_name,
            worker_id: None,
            status: SessionStatus::Queued,
            stream_url: None,
            created_at: now_ms,
            updated_at: now_ms,
            failure_reason: None,
        }
    }
}

pub fn create_client_sessions(
    headers: HeaderMap
    Json(request): Json<session::CreateSessionRequest>,
) -> Result<Json<session::ClientSession>, (StatusCode, String)> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() )
        .ok_or((
            StatusCode::BAD_REQUEST,
            "Missing or invalid Idempotency-Key header".to_string(),
        ))?;

}
