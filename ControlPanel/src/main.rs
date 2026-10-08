// HTTP Request (from the browser) ----> Go to the middleware ----> Go to the Home Server ----> We will send the request to the queue (if queue is not full) ----> Game agent (websockets) will pick up the request and process it ----> Game agent will send the response back to the Home Server ----> Home Server will send the response back to the middleware ----> Middleware will send the response back to the browser

use axum::{routing::get, Json, Router};
use serde_json::json; // This is for the JSON response
use std::net::SocketAddr;

#[derive(Serialize)]
struct HealthCheckResponse {
    status: String,
}

fn main() {
    unimplemented!();
}
