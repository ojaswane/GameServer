// The main job is to let the client know that the server is available or not
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
struct Worker {
    id: String,
    status: WorkerStatus,
    max
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
enum WorkerStatus {
    Offline,
    Online,
}
