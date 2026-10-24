// This is for the webRTC signaling between browser and game agent so that
// agent knows where to send the data peer-to peer
use axum::{extract::{Path, State}, http::StatusCode, Json};
use crate::session::{ClientSession, SessionRegistry, SessionStatus};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub async fn signaling(
    State(sessions): State<SessionRegistry>,
    State(workers): State<WorkerRegistry>,
) {
    let mut client = sessions.read().await;
    let mut worker = workers.read().await;

    let worker_id = client.worker_id
}
