// ============================================================
// BITBURNER RUST REMOTE API v1.3.0 — avec pont IA (Bitburner <-> Perchance)
// ------------------------------------------------------------
// Fichier MODIFIÉ : remplace ton ancien src/main.rs par celui-ci.
// Nouveau : file d'attente IA accessible en WebSocket RPC + HTTP.
//
//   WebSocket RPC (même socket que pushFile/getFile) :
//     - "aiSubmit"  { text }            -> { jobId, status }
//     - "aiGet"     { jobId }           -> { jobId, status, input, output }
//     - "aiPoll"    {}                  -> { jobId, input } ou { empty:true }
//     - "aiResolve" { jobId, output }   -> "OK"
//
//   HTTP (pour Bitburner fetch() + page Perchance) :
//     POST /api/ai/submit        { "text": "..." }      -> { jobId, status }
//     GET  /api/ai/result/{jobId}                       -> { status, input, output }
//     GET  /api/ai/pending                              -> { jobId, input } ou { empty:true }
//     POST /api/ai/resolve       { jobId, output }      -> { ok:true }
//     GET  /api/ai/jobs                                 -> debug (50 derniers)
// ============================================================

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
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
    ai_jobs: Mutex<HashMap<String, AiJob>>,
    ai_counter: AtomicUsize,
}

#[derive(Clone)]
struct FileRecord {
    content: String,
    created_at: u64,
    modified_at: u64,
    accessed_at: u64,
}

// ---------------- IA ----------------

#[derive(Clone)]
struct AiJob {
    id: String,
    input: String,
    output: Option<String>,
    created_at: u64,
    updated_at: u64,
}

#[derive(Deserialize)]
struct AiSubmitBody {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    input: Option<String>,
    #[serde(default)]
    content: Option<String>,
}

#[derive(Deserialize)]
struct AiResolveBody {
    #[serde(default, rename = "jobId")]
    job_id: Option<String>,
    #[serde(default, rename = "id")]
    id_alias: Option<String>,
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    result: Option<String>,
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
        ai_jobs: Mutex::new(HashMap::new()),
        ai_counter: AtomicUsize::new(0),
    })
}

fn next_ai_id(state: &SharedState) -> String {
    let n = state.ai_counter.fetch_add(1, Ordering::SeqCst) + 1;
    format!("ai-{n}-{}", now_ms() % 100000)
}

// Crée un job IA, retourne son id. Erreur texte si entrée vide.
fn create_ai_job(state: &SharedState, input: String) -> Result<String, String> {
    let text = input.trim().to_string();
    if text.is_empty() {
        return Err("Empty text".to_string());
    }
    if text.len() > 4000 {
        return Err("Text too long (max 4000 chars)".to_string());
    }
    let id = next_ai_id(state);
    let ts = now_ms();
    let job = AiJob {
        id: id.clone(),
        input: text,
        output: None,
        created_at: ts,
        updated_at: ts,
    };
    match state.ai_jobs.lock() {
        Ok(mut jobs) => {
            // Petit ménage : garde max 200 jobs pour ne pas gonfler la RAM
            if jobs.len() >= 200 {
                let mut ids: Vec<(String, u64)> = jobs
                    .iter()
                    .map(|(k, j)| (k.clone(), j.created_at))
                    .collect();
                ids.sort_by_key(|(_, t)| *t);
                let drop_n = jobs.len().saturating_sub(150);
                for (old_id, _) in ids.into_iter().take(drop_n) {
                    jobs.remove(&old_id);
                }
            }
            jobs.insert(id.clone(), job);
            Ok(id)
        }
        Err(_) => Err("Storage lock error".to_string()),
    }
}

fn oldest_pending_job(state: &SharedState) -> Option<AiJob> {
    let jobs = state.ai_jobs.lock().ok()?;
    jobs.values()
        .filter(|j| j.output.is_none())
        .min_by_key(|j| j.created_at)
        .cloned()
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
        // IA : SOUMETTRE UN TEXTE (Bitburner -> serveur)
        // params: { text } -> { jobId, status }
        // ----------------------------------------------------
        "aiSubmit" => {
            let text = get_string(&params, "text")
                .or_else(|| get_string(&params, "input"))
                .or_else(|| get_string(&params, "content"))
                .unwrap_or_default();

            match create_ai_job(state, text) {
                Ok(job_id) => rpc_success(id, json!({ "jobId": job_id, "status": "pending" })),
                Err(msg) => rpc_error(id, msg),
            }
        }

        // ----------------------------------------------------
        // IA : LIRE LE RÉSULTAT (Bitburner poll ici)
        // params: { jobId } -> { jobId, status, input, output }
        // ----------------------------------------------------
        "aiGet" => {
            let job_id = get_string(&params, "jobId")
                .or_else(|| get_string(&params, "id"))
                .unwrap_or_default();

            if job_id.trim().is_empty() {
                return rpc_error(id, "Missing jobId");
            }

            let jobs = match state.ai_jobs.lock() {
                Ok(jobs) => jobs,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            match jobs.get(&job_id) {
                Some(job) => rpc_success(
                    id,
                    json!({
                        "jobId": job.id,
                        "status": if job.output.is_some() { "done" } else { "pending" },
                        "input": job.input,
                        "output": job.output.clone().unwrap_or_default(),
                    }),
                ),
                None => rpc_error(id, "Job not found"),
            }
        }

        // ----------------------------------------------------
        // IA : PRENDRE LA PLUS VIEILLE DEMANDE EN ATTENTE
        // (c'est la page Perchance qui appelle ça en boucle)
        // ----------------------------------------------------
        "aiPoll" => match oldest_pending_job(state) {
            Some(job) => rpc_success(
                id,
                json!({ "jobId": job.id, "input": job.input, "createdAt": job.created_at }),
            ),
            None => rpc_success(id, json!({ "empty": true })),
        },

        // ----------------------------------------------------
        // IA : DÉPOSER LA RÉPONSE DE L'IA (Perchance -> serveur)
        // params: { jobId, output } -> "OK"
        // ----------------------------------------------------
        "aiResolve" | "aiResult" => {
            let job_id = get_string(&params, "jobId")
                .or_else(|| get_string(&params, "id"))
                .unwrap_or_default();
            let output = get_string(&params, "output")
                .or_else(|| get_string(&params, "text"))
                .or_else(|| get_string(&params, "result"))
                .unwrap_or_default();

            if job_id.trim().is_empty() {
                return rpc_error(id, "Missing jobId");
            }
            if output.trim().is_empty() {
                return rpc_error(id, "Missing output");
            }

            let mut jobs = match state.ai_jobs.lock() {
                Ok(jobs) => jobs,
                Err(_) => return rpc_error(id, "Storage lock error"),
            };

            match jobs.get_mut(&job_id) {
                Some(job) => {
                    job.output = Some(output);
                    job.updated_at = now_ms();
                    rpc_success(id, json!("OK"))
                }
                None => rpc_error(id, "Job not found"),
            }
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
    let pending = state
        .ai_jobs
        .lock()
        .map(|jobs| jobs.values().filter(|j| j.output.is_none()).count())
        .unwrap_or(0);
    Json(json!({
        "status": "online",
        "gameConnected": state.connections.load(Ordering::SeqCst) > 0,
        "connections": state.connections.load(Ordering::SeqCst),
        "pendingRequests": 0,
        "aiPending": pending,
        "uptime": state.uptime.elapsed().as_secs_f64(),
        "version": "1.3.0",
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
        "version": "1.3.0",
        "language": "Rust",
        "framework": "Axum",
        "runtime": "Tokio",
        "websocket": "/",
        "gameConnected": state.connections.load(Ordering::SeqCst) > 0,
        "aiBridge": true,
        "status": "online"
    }))
}

// ---------------- IA : HTTP ----------------

// POST /api/ai/submit  { text } -> { jobId, status }
async fn ai_submit_http(
    State(state): State<SharedState>,
    Json(body): Json<AiSubmitBody>,
) -> impl IntoResponse {
    let text = body
        .text
        .or(body.input)
        .or(body.content)
        .unwrap_or_default();

    match create_ai_job(&state, text) {
        Ok(job_id) => (
            StatusCode::OK,
            Json(json!({ "jobId": job_id, "status": "pending" })),
        ),
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": msg })),
        ),
    }
}

// GET /api/ai/result/{jobId} -> { status, input, output }
async fn ai_result_http(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let jobs = match state.ai_jobs.lock() {
        Ok(jobs) => jobs,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Storage lock error" })),
            )
        }
    };
    match jobs.get(&job_id) {
        Some(job) => (
            StatusCode::OK,
            Json(json!({
                "jobId": job.id,
                "status": if job.output.is_some() { "done" } else { "pending" },
                "input": job.input,
                "output": job.output.clone().unwrap_or_default(),
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Job not found" })),
        ),
    }
}

// GET /api/ai/pending -> plus vieille demande sans réponse
async fn ai_pending_http(State(state): State<SharedState>) -> Json<Value> {
    match oldest_pending_job(&state) {
        Some(job) => Json(json!({
            "jobId": job.id,
            "input": job.input,
            "createdAt": job.created_at
        })),
        None => Json(json!({ "empty": true })),
    }
}

// POST /api/ai/resolve { jobId, output } -> { ok:true }
async fn ai_resolve_http(
    State(state): State<SharedState>,
    Json(body): Json<AiResolveBody>,
) -> impl IntoResponse {
    let job_id = body
        .job_id
        .or(body.id_alias)
        .unwrap_or_default();
    let output = body
        .output
        .or(body.text)
        .or(body.result)
        .unwrap_or_default();

    if job_id.trim().is_empty() || output.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing jobId/output" })),
        );
    }

    let mut jobs = match state.ai_jobs.lock() {
        Ok(jobs) => jobs,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Storage lock error" })),
            )
        }
    };
    match jobs.get_mut(&job_id) {
        Some(job) => {
            job.output = Some(output);
            job.updated_at = now_ms();
            (StatusCode::OK, Json(json!({ "ok": true })))
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Job not found" })),
        ),
    }
}

// GET /api/ai/jobs -> debug : 50 derniers jobs
async fn ai_jobs_http(State(state): State<SharedState>) -> Json<Value> {
    let mut list: Vec<Value> = match state.ai_jobs.lock() {
        Ok(jobs) => jobs
            .values()
            .map(|j| {
                json!({
                    "jobId": j.id,
                    "status": if j.output.is_some() { "done" } else { "pending" },
                    "input": j.input,
                    "output": j.output.clone().unwrap_or_default(),
                    "createdAt": j.created_at,
                })
            })
            .collect(),
        Err(_) => vec![],
    };
    list.sort_by_key(|v| v.get("createdAt").and_then(Value::as_u64).unwrap_or(0));
    list.truncate(50);
    Json(json!(list))
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
        // Pont IA : Bitburner <-> Perchance
        .route("/api/ai/submit", post(ai_submit_http))
        .route("/api/ai/result/{jobId}", get(ai_result_http))
        .route("/api/ai/pending", get(ai_pending_http))
        .route("/api/ai/resolve", post(ai_resolve_http))
        .route("/api/ai/jobs", get(ai_jobs_http))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(10000);

    let address = format!("0.0.0.0:{}", port);

    println!("==========================================");
    println!(" BITBURNER RUST REMOTE API v1.3.0 + AI BRIDGE");
    println!("==========================================");
    println!("Listening on {}", address);
    println!("WebSocket: /");
    println!("WebSocket: /ws");
    println!("HTTP: /api/ping");
    println!("HTTP: /api/status");
    println!("HTTP: /api/wakeup");
    println!("HTTP: /api/info");
    println!("HTTP: POST /api/ai/submit");
    println!("HTTP: GET  /api/ai/result/{{jobId}}");
    println!("HTTP: GET  /api/ai/pending");
    println!("HTTP: POST /api/ai/resolve");
    println!("HTTP: GET  /api/ai/jobs");
    println!("RPC: aiSubmit / aiGet / aiPoll / aiResolve");

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("Failed to bind port");

    axum::serve(listener, app)
        .await
        .expect("Server error");
}
