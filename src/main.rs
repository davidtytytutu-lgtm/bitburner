
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::Method,
    response::Response,
    routing::get,
    Json,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

const SERVER_NAME: &str = "Bitburner Rust Server";
const VERSION: &str = "1.1.2";

struct AppState {
    started_at: Instant,
    connections: AtomicUsize,
}

type SharedState = Arc<AppState>;

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn connected(state: &AppState) -> bool {
    state.connections.load(Ordering::SeqCst) > 0
}

// HTTP /
async fn root(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "name": SERVER_NAME,
        "status": "online",
        "version": VERSION,
        "language": "Rust",
        "websocket": "/ws",
        "gameConnected": connected(&state),
        "endpoints": [
            "/api/ping",
            "/api/status",
            "/api/wakeup",
            "/api/info",
            "/ws"
        ]
    }))
}

// WebSocket /ws
async fn websocket_route(
    State(state): State<SharedState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

// PING
async fn ping(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "status": "pong",
        "server": SERVER_NAME,
        "gameConnected": connected(&state),
        "timestamp": timestamp()
    }))
}

// STATUS
async fn status(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "status": "online",
        "gameConnected": connected(&state),
        "connections": state.connections.load(Ordering::SeqCst),
        "pendingRequests": 0,
        "uptime": state.started_at.elapsed().as_secs_f64(),
        "version": VERSION,
        "timestamp": timestamp()
    }))
}

// WAKEUP
async fn wakeup(State(state): State<SharedState>) -> Json<Value> {
    println!("[WAKEUP] Request received");

    Json(json!({
        "status": "awake",
        "message": "Bitburner Rust Server is awake",
        "gameConnected": connected(&state),
        "uptime": state.started_at.elapsed().as_secs_f64(),
        "timestamp": timestamp()
    }))
}

// INFO
async fn info(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "name": SERVER_NAME,
        "version": VERSION,
        "language": "Rust",
        "framework": "Axum",
        "runtime": "Tokio",
        "websocket": "enabled",
        "gameConnected": connected(&state),
        "status": "online"
    }))
}

// WebSocket handler
async fn handle_socket(mut socket: WebSocket, state: SharedState) {
    state.connections.fetch_add(1, Ordering::SeqCst);

    println!("[WS] Client connected");
    println!(
        "[WS] Active connections: {}",
        state.connections.load(Ordering::SeqCst)
    );

    while let Some(message) = socket.next().await {
        match message {
            Ok(Message::Text(text)) => {
                println!("[WS] Received: {}", text);

                // Parse incoming JSON for diagnostics
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    let method = value
                        .get("method")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");

                    let id = value
                        .get("id")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "none".to_string());

                    println!("[WS] Method: {} | ID: {}", method, id);
                }
            }

            Ok(Message::Ping(data)) => {
                if socket.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }

            Ok(Message::Pong(_)) => {
                println!("[WS] Pong received");
            }

            Ok(Message::Close(_)) => {
                println!("[WS] Client requested close");
                break;
            }

            Ok(Message::Binary(data)) => {
                println!("[WS] Binary message: {} bytes", data.len());
            }

            Err(error) => {
                println!("[WS] Error: {}", error);
                break;
            }
        }
    }

    state.connections.fetch_sub(1, Ordering::SeqCst);

    println!("[WS] Client disconnected");
    println!(
        "[WS] Active connections: {}",
        state.connections.load(Ordering::SeqCst)
    );
}

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "10000".to_string());

    let state = Arc::new(AppState {
        started_at: Instant::now(),
        connections: AtomicUsize::new(0),
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::OPTIONS,
        ])
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(root))
        .route("/ws", get(websocket_route))
        .route("/api/ping", get(ping))
        .route("/api/status", get(status))
        .route("/api/wakeup", get(wakeup))
        .route("/api/info", get(info))
        .with_state(state)
        .layer(cors);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("Failed to bind port");

    println!("========================================");
    println!("       BITBURNER RUST SERVER");
    println!("========================================");
    println!("Version: {}", VERSION);
    println!("Port: {}", port);
    println!("WebSocket: /ws");
    println!("Status: ONLINE");

    axum::serve(listener, app)
        .await
        .expect("Server failed");
}
