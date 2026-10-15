//! Session data exchanged between the browser, control panel, and game agent.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

/// In-memory mapping from an idempotency key to its created session.
/// Persist this mapping in the database for idempotency across server restarts.
pub type SessionRegistry = Arc<RwLock<HashMap<String, ClientSession>>>;

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

/// Request accepted from a browser.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionRequest {
    pub game_name: String,
}

/// Control panel's record for one browser game session.
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

pub async fn create_client_session(
    State(sessions): State<SessionRegistry>,
    headers: HeaderMap,
    Json(request): Json<CreateSessionRequest>,
) -> Result<Json<ClientSession>, (StatusCode, String)> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "Missing or invalid Idempotency-Key header".to_string(),
        ))?;

    // Keep lookup and insertion under one lock so simultaneous retries cannot create duplicate sessions.
    let mut sessions = sessions.write().await;
    if let Some(existing) = sessions.get(key) {
        if existing.game_name != request.game_name {
            return Err((
                StatusCode::CONFLICT,
                "Idempotency-Key was already used for a game".to_string(),
            ));
        }
        return Ok(Json(existing.clone()));
    }

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let id = format!(
        "session-{now_ms}-{}",
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    );
    let session = ClientSession::queued(id, request.game_name, now_ms);
    sessions.insert(key.to_owned(), session.clone());

    Ok(Json(session))
}
