// this is basically for the gameAgent data eg, worker1 : is offline or online , and how many max sessions it can handle and how many current sessions it has
use serde::Serialize;
use axum::{Json};

#[derive(Debug, Clone, Serialize)]
pub struct Worker {
    id: String,
    status: WorkerStatus,
    pub max_sessions: u32,
    pub active_sessions: u32,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerStatus {
    Offline,
    Online,
}

impl Worker {
    pub fn available_slots(&self) -> u32 {
        self.max_sessions.saturating_sub(self.active_sessions) as u32
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

pub async fn list_workers() -> Json<Vec<WorkerResponse>> {

    // Dummy worker data TODO: update this to fetch real worker data from the game agent
    let worker = Worker {
        id: "gaming-pc-1".to_string(),
        status: WorkerStatus::Online,
        max_sessions: 3,
        active_sessions: 1,
    };

    let available_slots = worker.available_slots();
    let response = WorkerResponse {
        id: worker.id,
        status: worker.status,
        max_sessions: worker.max_sessions,
        active_sessions: worker.active_sessions,
        available_slots,
    };

    Json(vec![response])
}