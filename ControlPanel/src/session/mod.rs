// to register the session id , the game name , the ip address and the port number of the client
use axum::Json;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Serialize)]
pub struct ClientSession {
    id: string,
    ip: string,
    game_name: string,
    status: Bool,
}
