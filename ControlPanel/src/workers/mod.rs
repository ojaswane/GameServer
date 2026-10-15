// this is basically for the gameAgent data eg, worker1 : is offline or online , and how many max sessions it can handle and how many current sessions it has
use axum::{extract::{Path, State}, http::StatusCode, Json};
use crate::session::{ClientSession, SessionRegistry, SessionStatus};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Worker {
    id: String,
    status: WorkerStatus,
    pub max_sessions: u32,
    pub active_sessions: u32,
    pub last_heartbeat: Instant,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerStatus {
    Offline,
    Online,
}

// Shared registry of workers (game agents)
pub type WorkerRegistry = Arc<RwLock<HashMap<String, Worker>>>;


impl Worker {
    pub fn available_slots(&self) -> u32 {
        self.max_sessions.saturating_sub(self.active_sessions) as u32
    }

    pub fn mark_offline_if_stale(&mut self, timeout: std::time::Duration) {
        if self.last_heartbeat.elapsed() > timeout {
            self.status = WorkerStatus::Offline;
        }
    }
}

#[derive(Serialize)]
pub struct WorkerResponse {
    pub id: String,
    pub status: WorkerStatus,
    pub max_sessions: u32,
    pub active_sessions: u32,
    pub available_slots: u32,
}

// register workers
#[derive(Deserialize)]
pub struct RegisterWorkerRequest {
    pub id: String,
    pub max_sessions: u32,
}

// This is to write the game agent data into the hashmap to register the entry (for the v1 , we will have only one game agent)
pub async fn register_worker(
    State(workers): State<WorkerRegistry>,
    Json(request): Json<RegisterWorkerRequest>,
) -> Result<Json<WorkerResponse>, (StatusCode, String)> {
    let mut workers = workers.write().await;

    if request.max_sessions == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "max_sessions must be greater than zero".to_string(),
        ));
    }

    if workers.contains_key(&request.id) {
        return Err((
            StatusCode::CONFLICT,
            format!("Worker with id '{}' already exists", request.id),
        ));
    }


    let worker = Worker {
        id: request.id.clone(),
        status: WorkerStatus::Online,
        max_sessions: request.max_sessions,
        active_sessions: 0,
        last_heartbeat: Instant::now(),
    };

    let response = WorkerResponse {
        id: worker.id.clone(),
        status: worker.status.clone(),
        max_sessions: worker.max_sessions,
        active_sessions: worker.active_sessions,
        available_slots: worker.available_slots(),
    };

    workers.insert(request.id.clone(), worker);

    Ok(Json(response))
}

pub async fn list_workers(
    State(workers): State<WorkerRegistry>,
) -> Json<Vec<WorkerResponse>> {
    let workers = workers.read().await;

    let response: Vec<WorkerResponse> = workers
        .values()
        .map(|worker| WorkerResponse {
            id: worker.id.clone(),
            status: worker.status.clone(),
            max_sessions: worker.max_sessions,
            active_sessions: worker.active_sessions,
            available_slots: worker.available_slots(),
        })
        .collect();

    Json(response)
}

//heartbeat response to check if the game agent is online or offline
#[derive(Serialize)]
pub struct HeartbeatResponse {
    pub status: String,
}

pub async fn heartbeat(
    axum::extract::Path(worker_id): axum::extract::Path<String>,
    State(registry): State<WorkerRegistry>,
) -> Result<Json<HeartbeatResponse>, (StatusCode, String)> {
    let mut workers = registry.write().await;

    let worker = workers
        .get_mut(&worker_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("Worker '{worker_id}' was not found"),
            )
        })?;

    worker.last_heartbeat = Instant::now();
    worker.status = WorkerStatus::Online;

    Ok(Json(HeartbeatResponse {
        status: "heartbeat_received".to_string(),
    }))

}


/// Worker polls this endpoint to claim one queued session to start.
/// Session assignment and slot reservation happen together while holding both
/// registries, so competing workers cannot claim the same session.
pub async fn game_setup(
    Path(worker_id): Path<String>,
    State((workers, sessions)): State<(WorkerRegistry, SessionRegistry)>,
) -> Result<Json<Option<ClientSession>>, (StatusCode, String)> {
    let mut workers = workers.write().await;
    let worker = workers.get_mut(&worker_id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, format!("Worker '{worker_id}' was not found"))
    })?;

    if worker.status != WorkerStatus::Online {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "Worker is offline".to_string()));
    }
    
    if worker.available_slots() == 0 {
        return Err((StatusCode::CONFLICT, "Worker has no available session slots".to_string()));
    }

    let mut sessions = sessions.write().await;
    let Some(session) = sessions
        .values_mut()
        .find(|session| session.status == SessionStatus::Queued)
    else {
        return Ok(Json(None));
    };

    session.worker_id = Some(worker_id);
    session.status = SessionStatus::Starting;
    session.updated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    worker.active_sessions += 1;

    Ok(Json(Some(session.clone())))
}
