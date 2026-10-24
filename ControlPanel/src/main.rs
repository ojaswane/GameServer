// HTTP Request (from the browser) ----> Go to the middleware ----> Go to the Home Server ----> We will send the request to the queue (if queue is not full) ----> Game agent (websockets) will pick up the request and process it ----> Game agent will send the response back to the Home Server ----> Home Server will send the response back to the middleware ----> Middleware will send the response back to the browser

use axum::{Json, Router, routing::{get, post}};
use serde::Serialize;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use std::time::Duration;
use tokio::sync::RwLock;
use workers::WorkerRegistry;
mod session;
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

// monitor the heartbeat of the game agent
pub async fn monitor_workers(registry: WorkerRegistry) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        let mut workers = registry.write().await;

        // Mark workers offline after 30 seconds without a heartbeat.
        for worker in workers.values_mut() {
            worker.mark_offline_if_stale(Duration::from_secs(30));
        }
    }
}

#[tokio::main]
async fn main() {
    // Create one shared worker registry.
    let worker_registry: workers::WorkerRegistry =
        Arc::new(RwLock::new(HashMap::new()));
    let session_registry: session::SessionRegistry =
        Arc::new(RwLock::new(HashMap::new()));
        
    let monitor_registry = Arc::clone(&worker_registry);

    tokio::spawn(async move {
        monitor_workers(monitor_registry).await;
    });

    // Check the json response
    let worker_routes = Router::new()
        .route("/health", get(health_check))
        .route(
            "/workers",
            get(workers::list_workers).post(workers::register_worker),
        )
        .route("/workers/register", axum::routing::post(workers::register_worker))
        .route(
            "/workers/{worker_id}/heartbeat",
            axum::routing::post(workers::heartbeat),
        )
        .with_state(Arc::clone(&worker_registry));

    let session_routes = Router::new()
        .route("/sessions", post(session::create_client_session))
        .with_state(Arc::clone(&session_registry));

    let connection_info_routes = Router::new()
        .route(
            "/sessions/{session_id}/connection-info",
            get(workers::session_connection_info),
        )
        .with_state((Arc::clone(&worker_registry), Arc::clone(&session_registry)));

    let setup_routes = Router::new()
        .route(
            "/workers/{worker_id}/game-setup",
            post(workers::game_setup),
        )
        .with_state((worker_registry, session_registry));

    let app = worker_routes
        .merge(session_routes)
        .merge(connection_info_routes)
        .merge(setup_routes);

    let address = SocketAddr::from(([127, 0, 0, 1], 3000)); // that 3000 is the port number

    println!(" server running on http://{}", address);

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .unwrap();

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .await
        .unwrap();
}
