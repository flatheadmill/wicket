use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "wicket-wire-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("pane/test")).unwrap();
        Self(fs::canonicalize(root).unwrap())
    }
    fn intent(&self, bytes: &[u8]) -> Value {
        use sha2::{Digest, Sha256};
        json!({ "destination": self.0.join("pane/test/capture.png"), "bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(bytes)),
            "mode": "viewport", "source_url": "https://example.test/", "captured_at": "2026-09-20T00:00:00Z" })
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn png() -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 1, 1);
        encoder.set_color(png::ColorType::Rgba);
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&[1, 2, 3, 255]).unwrap();
        writer.finish().unwrap();
    }
    bytes
}
fn request(why: &str, sequence: u64, mut fields: Value) -> Value {
    fields["what"] = json!("screenshot_save");
    fields["why"] = json!(why);
    fields["sequence"] = json!(sequence);
    fields["binding"] = json!({ "operation": { "slug": "test", "caller": "shotgun", "operation_id": "op-1" },
        "attempt": { "call_id": "call-1", "attempt_id": "attempt-1" }, "source_socket": 1, "writer_socket": 2 });
    fields
}
async fn exchange(
    worker: &Worker,
    rx: &mut mpsc::Receiver<Completion>,
    packet: Value,
) -> Completion {
    assert!(worker
        .submit(Request::parse(&packet.to_string()).unwrap())
        .is_ok());
    tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn worker_is_lazy_and_round_trip_is_recoverable_without_raw_observations() {
    let fixture = Fixture::new();
    let bytes = png();
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let worker = Worker::start(fixture.0.clone(), tx);
    assert!(!fixture.0.join(".local").exists());
    let ready = exchange(
        &worker,
        &mut rx,
        request("begin", 1, json!({ "intent": fixture.intent(&bytes) })),
    )
    .await;
    assert_eq!(ready.packet["why"], "ready");
    assert!(ready.observations.is_empty());
    let journal = fixture.0.join(".local/state/wicket/screenshots");
    assert_eq!(fs::read_dir(&journal).unwrap().count(), 1); // lock only
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let ack = exchange(
        &worker,
        &mut rx,
        request("chunk", 2, json!({ "offset": 0, "data": encoded })),
    )
    .await;
    assert_eq!(ack.packet["why"], "ack");
    assert!(ack.observations.is_empty());
    let saved = exchange(&worker, &mut rx, request("finish", 3, json!({}))).await;
    assert_eq!(saved.packet["why"], "stored");
    assert_eq!(saved.observations.len(), 1);
    assert!(!serde_json::to_string(&saved.observations)
        .unwrap()
        .contains(&encoded));
    assert_eq!(
        fs::read(fixture.0.join("pane/test/capture.png")).unwrap(),
        bytes
    );
    rx.close();
    worker.shutdown().await;
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let worker = Worker::start(fixture.0.clone(), tx);
    let status = exchange(&worker, &mut rx, request("status", 1, json!({}))).await;
    assert_eq!(status.packet["receipt"], saved.packet["receipt"]);
    assert!(status.observations.is_empty());
    rx.close();
    worker.shutdown().await;
}

#[tokio::test]
async fn replacement_fences_old_socket_and_attempt_even_at_offset_zero() {
    let fixture = Fixture::new();
    let bytes = png();
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let worker = Worker::start(fixture.0.clone(), tx);
    exchange(
        &worker,
        &mut rx,
        request("begin", 1, json!({ "intent": fixture.intent(&bytes) })),
    )
    .await;
    let mut replacement = request("begin", 2, json!({ "intent": fixture.intent(&bytes) }));
    replacement["binding"]["attempt"] = json!({ "call_id": "call-2", "attempt_id": "attempt-2" });
    replacement["binding"]["source_socket"] = json!(3);
    assert_eq!(
        exchange(&worker, &mut rx, replacement.clone()).await.packet["why"],
        "ready"
    );
    let stale = exchange(&worker, &mut rx, request("cancel", 3, json!({}))).await;
    assert_eq!(stale.packet["why"], "rejected");
    assert!(stale.observations.is_empty());
    replacement["why"] = json!("cancel");
    replacement["sequence"] = json!(3);
    replacement.as_object_mut().unwrap().remove("intent");
    let aborted = exchange(&worker, &mut rx, replacement).await;
    assert_eq!(aborted.packet["why"], "aborted");
    rx.close();
    worker.shutdown().await;
}

#[tokio::test]
async fn invalid_chunk_is_fenced_and_never_quoted() {
    let fixture = Fixture::new();
    let bytes = png();
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let worker = Worker::start(fixture.0.clone(), tx);
    exchange(
        &worker,
        &mut rx,
        request("begin", 1, json!({ "intent": fixture.intent(&bytes) })),
    )
    .await;
    let secret = "RAW_SCREENSHOT_SENTINEL%%%%";
    let result = exchange(
        &worker,
        &mut rx,
        request("chunk", 2, json!({ "offset": 0, "data": secret })),
    )
    .await;
    assert_eq!(result.packet["why"], "aborted");
    assert!(!result.packet.to_string().contains(secret));
    assert!(!serde_json::to_string(&result.observations)
        .unwrap()
        .contains(secret));
    assert!(!fixture.0.join("pane/test/capture.png").exists());
    rx.close();
    worker.shutdown().await;
}

#[tokio::test]
async fn store_open_failure_is_a_capability_result_and_can_be_retried() {
    let fixture = Fixture::new();
    let lock = Store::open(&fixture.0).unwrap();
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let worker = Worker::start(fixture.0.clone(), tx);
    let result = exchange(&worker, &mut rx, request("status", 1, json!({}))).await;
    assert_eq!(result.packet["why"], "unresolved");
    drop(lock);
    let result = exchange(&worker, &mut rx, request("status", 2, json!({}))).await;
    assert_eq!(result.packet["why"], "unknown");
    rx.close();
    worker.shutdown().await;
}

#[test]
fn request_limits_and_schema_errors_do_not_quote_payloads() {
    let mut packet = request(
        "chunk",
        1,
        json!({ "offset": 0, "data": "A".repeat(MAX_PACKET_BYTES) }),
    );
    assert_eq!(
        Request::parse(&packet.to_string()).err(),
        Some("screenshot packet exceeds limit")
    );
    packet["data"] = json!("RAW_SCREENSHOT_SENTINEL");
    packet["surprise"] = json!(true);
    assert_eq!(
        Request::parse(&packet.to_string()).err(),
        Some("invalid screenshot command")
    );
}

#[tokio::test]
async fn status_reopens_a_frozen_idle_capability_without_inventing_failure() {
    use sha2::{Digest, Sha256};
    let fixture = Fixture::new();
    let bytes = png();
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let worker = Worker::start(fixture.0.clone(), tx);
    exchange(
        &worker,
        &mut rx,
        request("begin", 1, json!({ "intent": fixture.intent(&bytes) })),
    )
    .await;
    exchange(
        &worker,
        &mut rx,
        request(
            "chunk",
            2,
            json!({ "offset": 0, "data": base64::engine::general_purpose::STANDARD.encode(bytes) }),
        ),
    )
    .await;
    let pending = fixture
        .0
        .join(".local/state/wicket/screenshots")
        .join(format!("{:x}.new", Sha256::digest(b"test\0op-1")));
    fs::write(pending, b"interrupted journal preparation").unwrap();
    assert_eq!(
        exchange(&worker, &mut rx, request("finish", 3, json!({})))
            .await
            .packet["why"],
        "unresolved"
    );
    // Acceptance's rename never happened, so reopening establishes absence of
    // a durable operation. It must not report a failed published artifact.
    assert_eq!(
        exchange(&worker, &mut rx, request("status", 4, json!({})))
            .await
            .packet["why"],
        "unknown"
    );
    assert!(!fixture.0.join("pane/test/capture.png").exists());
    rx.close();
    worker.shutdown().await;
}
