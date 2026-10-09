// this is basically for the gameAgent data eg, worker1 : is offline or online , and how many max sessions it can handle and how many current sessions it has
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
struct Worker {
    id: String,
    status: WorkerStatus,
    max_sessions: usize,
    current_sessions: usize,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
enum WorkerStatus {
    Offline,
    Online,
}
