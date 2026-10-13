// to register the session id , the game name , the ip address and the port number of the client
use axum::Json;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionStatus {
    Pending,
    Ready,
    Running,
    Closed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSession {
    pub id: String,
    pub ip: String,
    pub game_name: String,
    pub worker_id: Option<String>,
    pub status: SessionStatus,
    pub created_at: u128,
    pub updated_at: u128,
}
