//! Session data exchanged between the browser, control panel, and game agent.

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
    /// The session ID also serves as the client-side session identity for routing.
    pub client_id: String,
    /// Remote address observed by the Control Panel when the session was created.
    pub client_ip: String,
    pub game_name: String,
    pub worker_id: Option<String>,
    pub status: SessionStatus,
    pub stream_url: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub failure_reason: Option<String>,
}

impl ClientSession {
    pub fn queued(id: String, client_ip: String, game_name: String, now_ms: u64) -> Self {
        Self {
            client_id: id.clone(),
            id,
            client_ip,
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
    axum::extract::ConnectInfo(remote): axum::extract::ConnectInfo<SocketAddr>,
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
            tracing::warn!(
                session_id = %existing.id,
                game_name = %request.game_name,
                "session retry rejected: idempotency key used for another game"
            );
            return Err((
                StatusCode::CONFLICT,
                "Idempotency-Key was already used for a game".to_string(),
            ));
        }
        tracing::debug!(session_id = %existing.id, "idempotent session retry returned existing session");
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
    let session = ClientSession::queued(id, remote.ip().to_string(), request.game_name, now_ms);
    
    check_worker_availability(
        session.worker_id.as_deref().unwrap_or_default(),
        &sessions,
    )?;

    tracing::info!(
        session_id = %session.id,
        game_name = %session.game_name,
        remote_ip = %remote.ip(),
        "client session created"
    );
    sessions.insert(key.to_owned(), session.clone());

    Ok(Json(session))
}


// once the SDP offer is recived , we will check that the worker is available or not
// if the worker is not available then we will return the error to the client and the client session will be removed from registry

pub fn check_worker_availability(
    worker_id: &str,
    workers: &WorkerRegistry,
) -> Result<(), (StatusCode, String)> {

    // to finish the check and block the thread of this until the worker is found thats why we use the futures::executor::block_on to block the thread until the worker is found
    let workers = futures::executor::block_on(workers.read());
    let worker = workers.get(worker_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("Worker '{worker_id}' was not found"),
        )
    })?;

    if worker.status != WorkerStatus::Online {
        tracing::warn!(worker_id = %worker_id, "Worker is offline");

        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Worker is offline".to_string(),
        ));
    }

    if worker.available_slots() == 0 {
        tracing::warn!(worker_id = %worker_id, "Worker has no available session slots");
        return Err((
            StatusCode::CONFLICT,
            "Worker has no available session slots".to_string(),
        ));
    }

    // TODO: later send the data through this signaling server
    tracing::info!(worker_id = %worker_id, "Worker is available for session assignment");
    Ok(())
}