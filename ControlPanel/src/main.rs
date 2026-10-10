// HTTP Request (from the browser) ----> Go to the middleware ----> Go to the Home Server ----> We will send the request to the queue (if queue is not full) ----> Game agent (websockets) will pick up the request and process it ----> Game agent will send the response back to the Home Server ----> Home Server will send the response back to the middleware ----> Middleware will send the response back to the browser

use axum::{Json, Router, routing::get};
use serde::Serialize;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
mod workers;

#[derive(Serialize)]
struct HealthCheckResponse {
    status: String,
}


// when client sents the Request , to check the json response
async fn health_check() -> Json<HealthCheckResponse> {
    let response = HealthCheckResponse {
        status: "OK".to_string(),
    };
    Json(response)
}


#[tokio::main]
async fn main() {
    // Create one shared worker registry.
    let worker_registry: workers::WorkerRegistry =
        Arc::new(RwLock::new(HashMap::new()));

    // Check the json response
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/workers", get(workers::list_workers))
        .with_state(worker_registry);

    let address = SocketAddr::from(([127, 0, 0, 1], 3000)); // that 3000 is the port number

    println!(" server running on http://{}", address);

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .unwrap();

    axum::serve(listener, app)
        .await
        .unwrap();
}
