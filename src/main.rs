
use axum::{
    http::Method,
    routing::get,
    Json,
    Router,
};
use serde_json::{json, Value};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

const SERVER_NAME: &str = "Bitburner Rust Server";
const VERSION: &str = "1.1.0";

#[derive(Clone)]
struct AppState {
    started_at: Instant,
}

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

async fn root() -> Json<Value> {
    Json(json!({
        "name": SERVER_NAME,
        "status": "online",
        "version": VERSION,
        "language": "Rust",
        "websocket": "/ws",
        "gameConnected": false,
        "endpoints": [
            "/api/ping",
            "/api/status",
            "/api/wakeup",
            "/api/info"
        ]
    }))
}

async fn ping() -> Json<Value> {
    Json(json!({
        "status": "pong",
        "server": SERVER_NAME,
        "gameConnected": false,
        "timestamp": timestamp()
    }))
}

async fn status(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<Value> {
    Json(json!({
        "status": "online",
        "gameConnected": false,
        "pendingRequests": 0,
        "uptime": state.started_at.elapsed().as_secs_f64(),
        "version": VERSION,
        "timestamp": timestamp()
    }))
}

async fn wakeup(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<Value> {
    println!("[WAKEUP] Wake-up request received");

    Json(json!({
        "status": "awake",
        "message": "Bitburner Rust Server is awake",
        "gameConnected": false,
        "uptime": state.started_at.elapsed().as_secs_f64(),
        "timestamp": timestamp()
    }))
}

async fn info() -> Json<Value> {
    Json(json!({
        "name": SERVER_NAME,
        "version": VERSION,
        "language": "Rust",
        "framework": "Axum",
        "runtime": "Tokio",
        "websocket": "WSS (planned)",
        "status": "online"
    }))
}

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "10000".to_string());

    let state = AppState {
        started_at: Instant::now(),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(root))
        .route("/api/ping", get(ping))
        .route("/api/status", get(status))
        .route("/api/wakeup", get(wakeup))
        .route("/api/info", get(info))
        .with_state(state)
        .layer(cors);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("Failed to bind port");

    println!("==================================");
    println!("  BITBURNER RUST SERVER");
    println!("==================================");
    println!("Version: {}", VERSION);
    println!("Port: {}", port);
    println!("Status: ONLINE");

    axum::serve(listener, app)
        .await
        .expect("Server failed");
}
