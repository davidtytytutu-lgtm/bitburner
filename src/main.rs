use axum::{
    routing::get,
    Router,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "10000".to_string());

    let app = Router::new()
        .route("/", get(|| async { "Bitburner Rust Server ONLINE" }))
        .route("/api/ping", get(|| async { "pong" }));

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();

    println!("Bitburner Rust Server started");

    axum::serve(listener, app)
        .await
        .unwrap();
}