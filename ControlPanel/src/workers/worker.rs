// this is basically for the gameAgent data eg, worker1 : is offline or online , and how many max sessions it can handle and how many current sessions it has
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Worker {
    id: String,
    status: WorkerStatus,
    pub max_sessions: usize,
    pub active_sessions: usize,
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
