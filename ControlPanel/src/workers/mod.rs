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

// Shared registry of workers (game agents)
pub type WorkerRegistry = Arc<RwLock<HashMap<String, Worker>>>;


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


#[derive(Serialize)]
pub struct WorkerResponse{
    pub id: String,
    pub status: WorkerStatus,
    pub max_sessions: u32,
    pub active_sessions: u32,
    pub available_slots: u32,
}

pub async fn list_workers(
    State(workers): State<WorkerRegistry>,
) -> Json<Vec<WorkerResponse>> {
    let workers = workers.read().await?;

    let available_slots = worker.available_slots();
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