//! Minimal authenticated ACP/JSON-RPC WebSocket mock for connector tests.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_hdr_async, WebSocketStream};

/// Start a mock Grok-like ACP server on an ephemeral loopback port.
pub async fn start_mock_acp_server(secret: &str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let secret = secret.to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let secret = secret.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, secret).await {
                    eprintln!("mock client error: {e}");
                }
            });
        }
    });
    addr
}

struct AuthCallback {
    secret: String,
}

impl Callback for AuthCallback {
    fn on_request(
        self,
        request: &Request,
        response: Response,
    ) -> Result<Response, ErrorResponse> {
        let uri = request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("");
        let ok = uri.contains(&format!("token={}", self.secret))
            || request
                .headers()
                .get("x-secret-key")
                .and_then(|v| v.to_str().ok())
                == Some(self.secret.as_str());
        if ok {
            Ok(response)
        } else {
            Err(http::Response::builder()
                .status(401)
                .body(Some("unauthorized".into()))
                .expect("response"))
        }
    }
}

async fn handle_client(stream: TcpStream, secret: String) -> Result<(), String> {
    let ws = accept_hdr_async(stream, AuthCallback { secret })
        .await
        .map_err(|e| format!("accept: {e}"))?;
    run_session(ws).await
}

async fn run_session(mut ws: WebSocketStream<TcpStream>) -> Result<(), String> {
    let sessions: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let next_session = AtomicU64::new(1);

    while let Some(msg) = ws.next().await {
        let msg = msg.map_err(|e| format!("read: {e}"))?;
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Ping(p) => {
                ws.send(Message::Pong(p))
                    .await
                    .map_err(|e| format!("pong: {e}"))?;
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };

        let req: Value = serde_json::from_str(&text).map_err(|e| format!("json: {e}"))?;
        let id = req.get("id").cloned();
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(Value::Null);

        let response = match method {
            "initialize" => {
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": params.get("protocolVersion").cloned().unwrap_or(json!("1")),
                        "agentCapabilities": {
                            "loadSession": true
                        },
                        "agentInfo": {
                            "name": "mock-acp",
                            "version": "0.1.0"
                        }
                    }
                })
            }
            "session/new" => {
                let n = next_session.fetch_add(1, Ordering::SeqCst);
                let sid = format!("sess-{n}");
                sessions.lock().await.insert(sid.clone(), n);
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "sessionId": sid }
                })
            }
            "session/load" => {
                let sid = params
                    .get("sessionId")
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                if sid.is_empty() {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": -32602, "message": "missing sessionId" }
                    })
                } else {
                    sessions.lock().await.insert(sid.to_string(), 0);
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": { "sessionId": sid }
                    })
                }
            }
            "session/prompt" => {
                let sid = params
                    .get("sessionId")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();
                let update = json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": "hello" }
                        }
                    }
                });
                ws.send(Message::Text(update.to_string().into()))
                    .await
                    .map_err(|e| format!("notify: {e}"))?;
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "stopReason": "end_turn"
                    }
                })
            }
            _ => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": "method not found" }
            }),
        };

        ws.send(Message::Text(response.to_string().into()))
            .await
            .map_err(|e| format!("write: {e}"))?;
    }
    Ok(())
}
