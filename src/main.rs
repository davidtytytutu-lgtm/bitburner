
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{IntoResponse, Response},
    routing::get,
    Json,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tower_http::cors::CorsLayer;

type SharedState = Arc<AppState>;

struct AppState {
    connections: AtomicUsize,
    uptime: Instant,
    files: Mutex<HashMap<String, HashMap<String, FileRecord>>>,
}

#[derive(Clone)]
struct FileRecord {
    content: String,
    created_at: u64,
    modified_at: u64,
    accessed_at: u64,
}

// ============================================================
// TIME
// ============================================================

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================
// STATE
// ============================================================

fn new_state() -> SharedState {
    Arc::new(AppState {
        connections: AtomicUsize::new(0),
        uptime: Instant::now(),
        files: Mutex::new(HashMap::new()),
    })
}

// ============================================================
// JSON-RPC HELPERS
// ============================================================

fn get_string(params: &Value, key: &str) -> Option<String> {
    params.get(key)?.as_str().map(String::from)
}

fn rpc_success(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn rpc_error(id: Value, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32000,
            "message": message.into()
        }
    })
}

fn metadata(filename: &str, record: &FileRecord) -> Value {
    json!({
        "filename": filename,
        "size": record.content.len(),
        "atime": record.accessed_at,
        "btime": record.created_at,
        "mtime": record.modified_at
    })
}

// ============================================================
// REMOTE API
// ============================================================

fn handle_rpc(state: &SharedState, request: Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);

    let method = match request.get("method").and_then(Value::as_str) {
        Some(method) => method,
        None => return rpc_error(id, "Missing method"),
    };

    let params = request
        .get("params")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let server = get_string(&params, "server")
        .unwrap_or_else(|| "home".to_string());

    let filename = get_string(&params, "filename");

    match method {
        // ----------------------------------------------------
        // UPLOAD FILE
        // ----------------------------------------------------
        "pushFile" => {
            let filename = match filename {
                Some(name) if !name.trim().is_empty() => name,
                _ => return rpc_error(id, "Invalid filename"),
            };

            let content = match get_string(&params, "content") {
                Some(content) => content,
                None => return rpc_error(id, "Missing content"),
            };

            let mut storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            let files = storage.entry(server).or_default();
            let timestamp = now_ms();

            if let Some(existing) = files.get_mut(&filename) {
                existing.content = content;
                existing.modified_at = timestamp;
                existing.accessed_at = timestamp;
            } else {
                files.insert(
                    filename,
                    FileRecord {
                        content,
                        created_at: timestamp,
                        modified_at: timestamp,
                        accessed_at: timestamp,
                    },
                );
            }

            rpc_success(id, json!("OK"))
        }

        // ----------------------------------------------------
        // GET FILE
        // ----------------------------------------------------
        "getFile" => {
            let filename = match filename {
                Some(name) => name,
                None => return rpc_error(id, "Missing filename"),
            };

            let mut storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            match storage
                .get_mut(&server)
                .and_then(|files| files.get_mut(&filename))
            {
                Some(file) => {
                    file.accessed_at = now_ms();
                    rpc_success(id, json!(file.content))
                }
                None => rpc_success(id, json!("")),
            }
        }

        // ----------------------------------------------------
        // FILE METADATA
        // ----------------------------------------------------
        "getFileMetadata" => {
            let filename = match filename {
                Some(name) => name,
                None => return rpc_error(id, "Missing filename"),
            };

            let storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            match storage
                .get(&server)
                .and_then(|files| files.get(&filename))
            {
                Some(file) => rpc_success(id, metadata(&filename, file)),
                None => rpc_error(id, "File not found"),
            }
        }

        // ----------------------------------------------------
        // DELETE FILE
        // ----------------------------------------------------
        "deleteFile" => {
            let filename = match filename {
                Some(name) => name,
                None => return rpc_error(id, "Missing filename"),
            };

            let mut storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            if let Some(files) = storage.get_mut(&server) {
                files.remove(&filename);
            }

            rpc_success(id, json!("OK"))
        }

        // ----------------------------------------------------
        // FILE NAMES
        // ----------------------------------------------------
        "getFileNames" => {
            let storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            let names: Vec<String> = storage
                .get(&server)
                .map(|files| files.keys().cloned().collect())
                .unwrap_or_default();

            rpc_success(id, json!(names))
        }

        // ----------------------------------------------------
        // ALL FILES
        // ----------------------------------------------------
        "getAllFiles" => {
            let storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            let all: Vec<Value> = storage
                .get(&server)
                .map(|files| {
                    files
                        .iter()
                        .map(|(filename, file)| {
                            json!({
                                "filename": filename,
                                "content": file.content
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            rpc_success(id, json!(all))
        }

        // ----------------------------------------------------
        // ALL FILE METADATA
        // ----------------------------------------------------
        "getAllFileMetadata" => {
            let storage = match state.files.lock() {
                Ok(storage) => storage,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            let all: Vec<Value> = storage
                .get(&server)
                .map(|files| {
                    files
                        .iter()
                        .map(|(filename, file)| metadata(filename, file))
                        .collect()
                })
                .unwrap_or_default();

            rpc_success(id, json!(all))
        }

        // ----------------------------------------------------
        // METHODS REQUIRING THE GAME ENGINE
        // ----------------------------------------------------
        "calculateRam" => {
            rpc_error(id, "calculateRam is unavailable on this standalone server")
        }

        "getDefinitionFile" => {
            rpc_error(id, "Game definitions are unavailable")
        }

        "getSaveFile" => {
            rpc_error(id, "Game save data is unavailable")
        }

        "getAllServers" => {
            rpc_error(id, "Game server data is unavailable")
        }

        // ----------------------------------------------------
        // UNKNOWN METHOD
        // ----------------------------------------------------
        _ => {
            rpc_error(id, format!("Unknown method: {}", method))
        }
    }
}

// ============================================================
// WEBSOCKET
// ============================================================

async fn websocket_handler(
    State(state): State<SharedState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

async fn handle_socket(mut socket: WebSocket, state: SharedState) {
    state.connections.fetch_add(1, Ordering::SeqCst);

    println!("[WS] Client connected");
    println!(
        "[WS] Active connections: {}",
        state.connections.load(Ordering::SeqCst)
    );

    while let Some(message) = socket.next().await {
        match message {
            // ------------------------------------------------
            // JSON REQUEST
            // ------------------------------------------------
            Ok(Message::Text(text)) => {
                println!("[RPC] Received: {}", text);

                let request: Value = match serde_json::from_str(text.as_str()) {
                    Ok(value) => value,
                    Err(error) => {
                        println!("[RPC] Invalid JSON: {}", error);
                        continue;
                    }
                };

                let response = handle_rpc(&state, request);
                let response_text = response.to_string();

                println!("[RPC] Response: {}", response_text);

                if socket
                    .send(Message::Text(response_text.into()))
                    .await
                    .is_err()
                {
                    println!("[WS] Failed to send response");
                    break;
                }
            }

            // ------------------------------------------------
            // PING
            // ------------------------------------------------
            Ok(Message::Ping(data)) => {
                if socket.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }

            // ------------------------------------------------
            // PONG
            // ------------------------------------------------
            Ok(Message::Pong(_)) => {}

            // ------------------------------------------------
            // DISCONNECT
            // ------------------------------------------------
            Ok(Message::Close(_)) => {
                println!("[WS] Client requested close");
                break;
            }

                       // OTHER MESSAGE
            Ok(_) => {}

            // ERROR
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
// ============================================================
// HTTP API
// ============================================================

async fn ping() -> Json<Value> {
    Json(json!({
        "status": "pong",
        "server": "Bitburner Rust Remote API",
        "timestamp": now_ms()
    }))
}

async fn status(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "status": "online",
        "gameConnected": state.connections.load(Ordering::SeqCst) > 0,
        "connections": state.connections.load(Ordering::SeqCst),
        "pendingRequests": 0,
        "uptime": state.uptime.elapsed().as_secs_f64(),
        "version": "1.2.2",
        "timestamp": now_ms()
    }))
}

async fn wakeup(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "status": "awake",
        "gameConnected": state.connections.load(Ordering::SeqCst) > 0,
        "uptime": state.uptime.elapsed().as_secs_f64(),
        "timestamp": now_ms()
    }))
}

async fn info(State(state): State<SharedState>) -> Json<Value> {
    Json(json!({
        "name": "Bitburner Rust Remote API",
        "version": "1.2.2",
        "language": "Rust",
        "framework": "Axum",
        "runtime": "Tokio",
        "websocket": "/",
        "gameConnected": state.connections.load(Ordering::SeqCst) > 0,
        "status": "online"
    }))
}

// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main() {
    let state = new_state();

    let app = Router::new()
        // Remote API WebSocket root
        .route("/", get(websocket_handler))
        // Alternate WebSocket endpoint
        .route("/ws", get(websocket_handler))
        // Diagnostic HTTP endpoints
        .route("/api/ping", get(ping))
        .route("/api/status", get(status))
        .route("/api/wakeup", get(wakeup))
        .route("/api/info", get(info))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(10000);

    let address = format!("0.0.0.0:{}", port);

    println!("==========================================");
    println!(" BITBURNER RUST REMOTE API v1.2.2");
    println!("==========================================");
    println!("Listening on {}", address);
    println!("WebSocket: /");
    println!("WebSocket: /ws");
    println!("HTTP: /api/ping");
    println!("HTTP: /api/status");
    println!("HTTP: /api/wakeup");
    println!("HTTP: /api/info");

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("Failed to bind port");

    axum::serve(listener, app)
        .await
        .expect("Server error");
}
