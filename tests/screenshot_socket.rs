//! Exercise the actual Wicket reader, blocking owner, and shared-log path.
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

fn request(why: &str, sequence: u64, mut fields: Value) -> Value {
    fields["what"] = json!("screenshot_save");
    fields["why"] = json!(why);
    fields["sequence"] = json!(sequence);
    fields["binding"] = json!({ "operation": { "slug": "test", "caller": "shotgun", "operation_id": "socket-save" },
        "attempt": { "call_id": "call-1", "attempt_id": "attempt-1" }, "source_socket": 1, "writer_socket": 2 });
    fields
}

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn actual_wicket_saves_exact_bytes_and_keeps_chunks_out_of_both_logs() {
    let home = std::env::temp_dir().join(format!(
        "wicket-socket-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    tokio::fs::create_dir_all(home.join("pane/test"))
        .await
        .unwrap();
    let fixture = Fixture(tokio::fs::canonicalize(home).await.unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_wicket"))
        .args([format!("ws://127.0.0.1:{port}"), "localhost".into()])
        .env("HOME", &fixture.0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut socket = accept_async(stream).await.unwrap();
    let connect = socket.next().await.unwrap().unwrap().into_text().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&connect).unwrap()["who"],
        "wicket"
    );
    assert!(!fixture.0.join(".local/state/wicket/screenshots").exists());
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 1, 1);
        encoder.set_color(png::ColorType::Rgba);
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&[42, 51, 99, 255]).unwrap();
        writer.finish().unwrap();
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let destination = fixture.0.join("pane/test/capture.png");
    let manifest = json!({ "destination": destination, "bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes)),
        "mode": "viewport", "source_url": "https://example.test/", "captured_at": "now" });
    let mut observations = Vec::new();
    let mut stored = Value::Null;
    for (why, sequence, fields, expected) in [
        ("begin", 1, json!({ "intent": manifest }), "ready"),
        ("chunk", 2, json!({ "offset": 0, "data": encoded }), "ack"),
        ("finish", 3, json!({}), "stored"),
        // Cancellation that loses to publication must return the same receipt.
        ("cancel", 4, json!({}), "stored"),
    ] {
        socket
            .send(Message::text(request(why, sequence, fields).to_string()))
            .await
            .unwrap();
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(10), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            let packet: Value = serde_json::from_str(&text).unwrap();
            if packet["what"] == "log" {
                observations.push(packet);
                continue;
            }
            assert_eq!(packet["what"], "screenshot_save");
            assert_eq!(packet["why"], expected);
            if why == "finish" {
                stored = packet["receipt"].clone();
            }
            if why == "cancel" {
                assert_eq!(packet["receipt"], stored);
            }
            break;
        }
    }
    assert_eq!(tokio::fs::read(destination).await.unwrap(), bytes);
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0]["with"]["what"], "store");
    assert!(!serde_json::to_string(&observations)
        .unwrap()
        .contains(&encoded));
    socket.close(None).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    let local = tokio::fs::read_to_string(fixture.0.join(".local/state/wicket/localhost.jsonl"))
        .await
        .unwrap();
    assert!(!local.contains(&encoded));
}
