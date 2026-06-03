// Wicket: tool executor that connects back to Easement over WebSocket.
//
// Receives tool call envelopes (zsh, apply_patch, view_image, shell),
// executes them in a sandbox, returns results. One Wicket per host.
//
// Usage:
//   wicket ws://localhost:6502           # local executor
//   ssh host wicket ws://localhost:6502  # remote executor (via SSH tunnel)

use std::path::{Path, PathBuf};
use std::process::Stdio;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

// -- Logging --

fn init_tracing() -> WorkerGuard {
    let home = std::env::var("HOME").expect("HOME not set");
    let log_dir = Path::new(&home)
        .join(".local")
        .join("state")
        .join("wicket");
    let _ = std::fs::create_dir_all(&log_dir);

    let port: u16 = std::env::var("EASEMENT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6502);
    let log_name = if port == 6502 {
        "wicket.log".to_string()
    } else {
        format!("wicket-{}.log", port)
    };

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join(&log_name))
        .expect("failed to open log file");

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("wicket=debug"));

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .init();

    guard
}

// -- WebSocket send --

type WsSender = mpsc::UnboundedSender<String>;

fn ws_emit(tx: &WsSender, stream: &str, slug: &str, timestamp: &str, data: Value) {
    let envelope = json!({ "stream": stream, "slug": slug, "timestamp": timestamp, "data": data });
    if let Ok(json) = serde_json::to_string(&envelope) {
        log_wire("_", "send", &json);
        let _ = tx.send(json);
    }
}

fn log_wire(slug: &str, dir: &str, raw: &str) {
    let home = std::env::var("HOME").unwrap_or_default();
    let log_dir = Path::new(&home)
        .join(".local/state/wicket")
        .join(slug);
    let _ = std::fs::create_dir_all(&log_dir);
    let path = log_dir.join("wire.jsonl");
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let entry = json!({ "ts": now, "dir": dir, "raw": raw });
    if let Ok(mut line) = serde_json::to_string(&entry) {
        line.push('\n');
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            let _ = std::io::Write::write_all(&mut file, line.as_bytes());
        }
    }
}

// -- Sandbox --

struct SandboxConfig {
    writable: Vec<String>,
}

fn read_sandbox_config(slug: &str) -> SandboxConfig {
    let home = std::env::var("HOME").unwrap_or_default();
    let path = Path::new(&home)
        .join(".local/state/wicket")
        .join(slug)
        .join("sandbox.conf");

    let mut writable = Vec::new();

    if let Ok(content) = std::fs::read_to_string(&path) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(path) = line.strip_prefix("writable ") {
                let expanded = path.trim().replace("~/", &format!("{}/", home));
                writable.push(expanded);
            }
        }
    }

    SandboxConfig { writable }
}

#[cfg(target_os = "macos")]
fn build_sandbox_command(command: &str, config: &SandboxConfig) -> tokio::process::Command {
    let mut policy = String::new();
    policy.push_str("(version 1)\n");
    policy.push_str("(deny default)\n");
    policy.push_str("(allow process-exec)\n");
    policy.push_str("(allow process-fork)\n");
    policy.push_str("(allow signal (target same-sandbox))\n");
    policy.push_str("(allow process-info* (target same-sandbox))\n");
    policy.push_str("(allow file-read*)\n");
    for path in &config.writable {
        policy.push_str(&format!("(allow file-write* (subpath \"{}\"))\n", path));
    }
    policy.push_str("(allow file-write* (subpath \"/tmp\"))\n");
    policy.push_str("(allow file-write* (subpath \"/private/tmp\"))\n");
    policy.push_str(&format!(
        "(allow file-write* (subpath \"{}\"))\n",
        std::env::temp_dir().display()
    ));
    policy.push_str("(allow file-write-data (require-all (path \"/dev/null\") (vnode-type CHARACTER-DEVICE)))\n");
    policy.push_str("(allow pseudo-tty)\n");
    policy.push_str("(allow file-read* file-write* file-ioctl (literal \"/dev/ptmx\"))\n");
    policy.push_str("(allow file-read* file-write* (regex #\"^/dev/ttys[0-9]+\"))\n");
    policy.push_str("(allow file-ioctl (regex #\"^/dev/ttys[0-9]+\"))\n");
    policy.push_str("(allow sysctl-read)\n");
    policy.push_str("(allow mach-lookup)\n");
    policy.push_str("(allow network-outbound)\n");
    policy.push_str("(allow network-inbound)\n");
    policy.push_str("(allow system-socket)\n");
    policy.push_str("(allow ipc-posix-sem)\n");
    policy.push_str("(allow ipc-posix-shm-read*)\n");
    policy.push_str("(allow user-preference-read)\n");

    let mut cmd = tokio::process::Command::new("/usr/bin/sandbox-exec");
    cmd.arg("-p").arg(&policy).arg("--").arg("zsh").arg("-c").arg(command);
    cmd
}

#[cfg(target_os = "linux")]
fn build_sandbox_command(command: &str, config: &SandboxConfig) -> tokio::process::Command {
    let mut args = vec![
        "--new-session".to_string(),
        "--die-with-parent".to_string(),
        "--ro-bind".to_string(), "/".to_string(), "/".to_string(),
        "--dev".to_string(), "/dev".to_string(),
        "--proc".to_string(), "/proc".to_string(),
        "--tmpfs".to_string(), "/tmp".to_string(),
        "--unshare-pid".to_string(),
    ];
    for path in &config.writable {
        args.push("--bind".to_string());
        args.push(path.clone());
        args.push(path.clone());
    }
    args.push("--".to_string());
    args.push("zsh".to_string());
    args.push("-c".to_string());
    args.push(command.to_string());

    let mut cmd = tokio::process::Command::new("bwrap");
    cmd.args(&args);
    cmd
}

// -- Tool handlers --

async fn handle_zsh(tx: &WsSender, slug: &str, timestamp: &str, call_id: &str, host_identity: &str, data: Value) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    let sandboxed = data.get("sandboxed").and_then(|v| v.as_bool()).unwrap_or(true);
    let run_bg = data.get("run_in_background").and_then(|v| v.as_bool()).unwrap_or(false);
    let timestamp = data.get("timestamp").and_then(|v| v.as_str()).unwrap_or("default");
    let task_uuid = data.get("task_uuid").and_then(|v| v.as_str()).unwrap_or("");
    tracing::info!(command = %command, sandboxed, run_bg, "zsh exec");

    if run_bg {
        let home = std::env::var("HOME").unwrap_or_default();
        let output_dir = Path::new(&home)
            .join(".local/state/wicket")
            .join(slug)
            .join(timestamp)
            .join(host_identity);
        let _ = std::fs::create_dir_all(&output_dir);
        let output_path = output_dir.join(format!("{}.txt", task_uuid));

        let file = match std::fs::File::create(&output_path) {
            Ok(f) => f,
            Err(e) => {
                ws_emit(tx, "tool_result", slug, timestamp, json!({
                    "call_id": call_id,
                    "output": format!("failed to create output file: {}", e),
                    "exit_code": 1,
                }));
                return;
            }
        };
        let stdout_file = file.try_clone().unwrap();
        let stderr_file = file;

        let mut cmd = if sandboxed {
            let config = read_sandbox_config(slug);
            let mut c = build_sandbox_command(command, &config);
            c.env("RUNNING_UNDER_WICKET", "1");
            c
        } else {
            let mut c = tokio::process::Command::new("zsh");
            c.env("RUNNING_UNDER_WICKET", "1");
            c.arg("-c").arg(command);
            c
        };
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        match cmd.spawn() {
            Ok(mut child) => {
                ws_emit(tx, "tool_result", slug, timestamp, json!({
                    "call_id": call_id,
                    "output": format!("Background task {} started. Output: {}", task_uuid, output_path.display()),
                    "exit_code": 0,
                }));

                let tx = tx.clone();
                let output_path = output_path.clone();
                let task_uuid = task_uuid.to_string();
                let is_localhost = host_identity == "localhost";
                let bg_slug = slug.to_string();
                let bg_timestamp = timestamp.to_string();

                let child_stdout = child.stdout.take();

                tokio::spawn(async move {
                    use tokio::io::{AsyncBufReadExt, BufReader};

                    let mut file = std::fs::OpenOptions::new()
                        .create(true).append(true)
                        .open(&output_path)
                        .ok();

                    if let Some(stdout) = child_stdout {
                        let tx = tx.clone();
                        let task_uuid_clone = task_uuid.clone();
                        let mut reader = BufReader::new(stdout);
                        let mut line = String::new();
                        loop {
                            line.clear();
                            match reader.read_line(&mut line).await {
                                Ok(0) => break,
                                Ok(_) => {
                                    if let Some(ref mut f) = file {
                                        let _ = std::io::Write::write_all(f, line.as_bytes());
                                    }
                                    if !is_localhost {
                                        ws_emit(&tx, "background_output", &bg_slug, &bg_timestamp, json!({
                                            "task_uuid": task_uuid_clone,
                                            "output_path": output_path.to_string_lossy(),
                                            "line": line.trim_end(),
                                        }));
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }

                    let status = child.wait().await;
                    let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                    let output_path_str = output_path.to_string_lossy().to_string();
                    tracing::info!(task_uuid = %task_uuid, exit_code = code, "background task completed");
                    ws_emit(&tx, "background_done", &bg_slug, &bg_timestamp, json!({
                        "task_uuid": task_uuid,
                        "exit_code": code,
                        "output_path": output_path_str,
                    }));
                });
            }
            Err(e) => {
                ws_emit(tx, "tool_result", slug, timestamp, json!({
                    "call_id": call_id,
                    "output": format!("failed to spawn background task: {}", e),
                    "exit_code": 1,
                }));
            }
        }
        return;
    }

    let output = if sandboxed {
        let config = read_sandbox_config(slug);
        let mut cmd = build_sandbox_command(command, &config);
        cmd.env("RUNNING_UNDER_WICKET", "1");
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        cmd.output().await
    } else {
        tracing::warn!(command = %command, "running unsandboxed");
        let mut cmd = tokio::process::Command::new("zsh");
        cmd.env("RUNNING_UNDER_WICKET", "1");
        cmd.arg("-c").arg(command);
        cmd.output().await
    };

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            let combined = if stderr.is_empty() {
                stdout.to_string()
            } else {
                format!("{}{}", stdout, stderr)
            };
            let code = out.status.code().unwrap_or(-1);
            ws_emit(tx, "tool_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": combined,
                "exit_code": code,
            }));
        }
        Err(e) => {
            ws_emit(tx, "tool_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": format!("failed to execute: {}", e),
                "exit_code": 1,
            }));
        }
    }
}

async fn handle_shell(tx: &WsSender, slug: &str, timestamp: &str, call_id: &str, data: Value) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    tracing::info!(command = %command, "shell exec (unsandboxed)");

    let output = tokio::process::Command::new("zsh")
        .env("RUNNING_UNDER_WICKET", "1")
        .arg("-c")
        .arg(command)
        .output()
        .await;

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            let combined = if stderr.is_empty() {
                stdout.to_string()
            } else {
                format!("{}{}", stdout, stderr)
            };
            let code = out.status.code().unwrap_or(-1);
            ws_emit(tx, "shell_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": combined,
                "exit_code": code,
            }));
        }
        Err(e) => {
            ws_emit(tx, "shell_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": format!("failed to execute: {}", e),
                "exit_code": 1,
            }));
        }
    }
}

async fn handle_apply_patch(tx: &WsSender, slug: &str, timestamp: &str, call_id: &str, data: Value) {
    let patch = data.get("patch").and_then(|v| v.as_str()).unwrap_or("");
    tracing::info!("apply_patch request");

    let sandbox_config = read_sandbox_config(slug);
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    match codex_apply_patch::parse_patch(patch) {
        Ok(parsed) => {
            let mut denied_path = None;
            for hunk in &parsed.hunks {
                let path = match hunk {
                    codex_apply_patch::Hunk::AddFile { path, .. } => path,
                    codex_apply_patch::Hunk::DeleteFile { path } => path,
                    codex_apply_patch::Hunk::UpdateFile { path, .. } => path,
                };
                let abs = cwd.join(path).to_string_lossy().to_string();
                let is_writable = sandbox_config.writable.iter()
                    .any(|root| abs.starts_with(root));
                if !is_writable {
                    denied_path = Some(abs);
                    break;
                }
            }

            if let Some(denied) = denied_path {
                ws_emit(tx, "tool_result", slug, timestamp, json!({
                    "call_id": call_id,
                    "output": format!("patch denied: {} is not inside a writable root", denied),
                    "exit_code": 1,
                }));
            } else {
                let mut stdout_buf = Vec::new();
                let mut stderr_buf = Vec::new();
                match codex_apply_patch::apply_patch(patch, &mut stdout_buf, &mut stderr_buf) {
                    Ok(()) => {
                        let output = String::from_utf8_lossy(&stdout_buf);
                        ws_emit(tx, "tool_result", slug, timestamp, json!({
                            "call_id": call_id,
                            "output": output.trim_end(),
                            "exit_code": 0,
                        }));
                    }
                    Err(e) => {
                        let stderr_str = String::from_utf8_lossy(&stderr_buf);
                        let output = if stderr_str.is_empty() {
                            format!("patch failed: {}", e)
                        } else {
                            format!("{}\npatch failed: {}", stderr_str.trim_end(), e)
                        };
                        ws_emit(tx, "tool_result", slug, timestamp, json!({
                            "call_id": call_id,
                            "output": output,
                            "exit_code": 1,
                        }));
                    }
                }
            }
        }
        Err(e) => {
            ws_emit(tx, "tool_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": format!("patch parse error: {}", e),
                "exit_code": 1,
            }));
        }
    }
}

async fn handle_view_image(tx: &WsSender, slug: &str, timestamp: &str, call_id: &str, data: Value) {
    let path_str = data.get("path").and_then(|v| v.as_str()).unwrap_or("");
    tracing::info!(path = %path_str, "view_image");

    let path = Path::new(path_str);
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };

    let file_bytes = match std::fs::read(&abs_path) {
        Ok(b) => b,
        Err(e) => {
            ws_emit(tx, "tool_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": format!("cannot read image: {}", e),
                "exit_code": 1,
            }));
            return;
        }
    };

    let img = match image::load_from_memory(&file_bytes) {
        Ok(i) => i,
        Err(e) => {
            ws_emit(tx, "tool_result", slug, timestamp, json!({
                "call_id": call_id,
                "output": format!("cannot decode image: {}", e),
                "exit_code": 1,
            }));
            return;
        }
    };

    const MAX_DIM: u32 = 1568;
    let (w, h) = (img.width(), img.height());
    let needs_resize = w > MAX_DIM || h > MAX_DIM;

    let (output_bytes, output_w, output_h, media_type) = if needs_resize {
        let resized = img.resize(MAX_DIM, MAX_DIM, image::imageops::FilterType::Triangle);
        let (rw, rh) = (resized.width(), resized.height());
        let mut buf = std::io::Cursor::new(Vec::new());
        resized.write_to(&mut buf, image::ImageFormat::Jpeg)
            .unwrap_or_else(|e| tracing::warn!("jpeg encode failed: {}", e));
        (buf.into_inner(), rw, rh, "image/jpeg")
    } else {
        let ext = abs_path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let media_type = match ext.as_str() {
            "jpg" | "jpeg" => "image/jpeg",
            "png" => "image/png",
            "gif" => "image/gif",
            "webp" => "image/webp",
            _ => "image/png",
        };
        (file_bytes, w, h, media_type)
    };

    tracing::info!(
        original = format!("{}x{}", w, h),
        output = format!("{}x{}", output_w, output_h),
        resized = needs_resize,
        bytes = output_bytes.len(),
        "view_image encoded"
    );

    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&output_bytes);

    let content = json!([
        { "type": "text", "text": format!("{} ({}x{} {})", abs_path.display(), output_w, output_h, media_type) },
        { "type": "image", "data": encoded, "mimeType": media_type }
    ]);

    ws_emit(tx, "tool_result", slug, timestamp, json!({
        "call_id": call_id,
        "output": serde_json::to_string(&content).unwrap_or_default(),
        "exit_code": 0,
    }));
}

// -- Main --

#[tokio::main]
async fn main() {
    let _guard = init_tracing();

    let args: Vec<String> = std::env::args().collect();
    let wicket_url = args.get(1).cloned().unwrap_or_else(|| {
        eprintln!("usage: wicket <wicket-ws-url> <slug> [host-identity]");
        std::process::exit(1);
    });
    let slug = args.get(2).cloned().unwrap_or_else(|| "_wicket".to_string());
    let host_identity = args.get(3).cloned().unwrap_or_else(|| "localhost".to_string());

    tracing::info!(url = %wicket_url, slug = %slug, host = %host_identity, "wicket starting");

    let (ws_stream, _) = match tokio_tungstenite::connect_async(&wicket_url).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot connect to wicket");
            eprintln!("cannot connect to wicket: {}", e);
            std::process::exit(1);
        }
    };

    let (mut ws_sink, mut ws_stream_rx) = ws_stream.split();

    // No connect payload. Wicket receives bus messages and claims by who="wicket".

    tracing::info!("connected to wicket");

    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        while let Some(msg) = ws_rx.recv().await {
            if ws_sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
        let _ = ws_sink.close().await;
    });


    while let Some(result) = ws_stream_rx.next().await {
        match result {
            Ok(Message::Text(text)) => {
                log_wire("_", "recv", &text);

                let envelope: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("bad envelope: {}", e);
                        continue;
                    }
                };

                let stream = match envelope.get("stream").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => continue,
                };
                let data = envelope.get("data").cloned().unwrap_or_default();

                match stream {
                    "call" => {
                        let who = match data.get("who").and_then(|v| v.as_str()) {
                            Some(w) => w,
                            None => continue,
                        };
                        if who != "wicket" {
                            continue;
                        }

                        let slug = envelope.get("slug").and_then(|v| v.as_str())
                            .expect("call envelope missing slug");
                        let timestamp = envelope.get("timestamp").and_then(|v| v.as_str())
                            .expect("call envelope missing timestamp");
                        let f = data.get("f").and_then(|v| v.as_str())
                            .expect("call envelope missing f");
                        let call_id = data.get("id").and_then(|v| v.as_str())
                            .expect("call envelope missing id");
                        let args = data.get("args").cloned().unwrap_or_default();
                        let escalated = data.get("escalated").and_then(|v| v.as_bool()).unwrap_or(false);

                        let host = args.get("host").and_then(|v| v.as_str()).unwrap_or("localhost");
                        if host != host_identity {
                            continue;
                        }

                        tracing::info!(call_id = %call_id, f = %f, slug = %slug, "claimed call");
                        let call_id = call_id.to_string();

                        match f {
                            "zsh" => {
                                let zsh_data = json!({
                                    "command": args.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                                    "sandboxed": !escalated,
                                    "run_in_background": args.get("run_in_background").and_then(|v| v.as_bool()).unwrap_or(false),
                                    "timeout": args.get("timeout"),
                                    "task_uuid": call_id,
                                    "timestamp": timestamp,
                                });
                                handle_zsh(&ws_tx, slug, timestamp, &call_id, &host_identity, zsh_data).await;
                            }
                            "shell" => {
                                let shell_data = json!({
                                    "command": args.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                                });
                                handle_shell(&ws_tx, slug, timestamp, &call_id, shell_data).await;
                            }
                            "apply_patch" => {
                                let patch_data = json!({
                                    "patch": args.get("patch").and_then(|v| v.as_str()).unwrap_or(""),
                                });
                                handle_apply_patch(&ws_tx, slug, timestamp, &call_id, patch_data).await;
                            }
                            "view_image" => {
                                let image_data = json!({
                                    "path": args.get("path").and_then(|v| v.as_str()).unwrap_or(""),
                                });
                                handle_view_image(&ws_tx, slug, timestamp, &call_id, image_data).await;
                            }
                            _ => {
                                ws_emit(&ws_tx, "tool_result", slug, timestamp, json!({
                                    "call_id": call_id,
                                    "output": format!("unknown function: {}", f),
                                    "exit_code": 1,
                                }));
                            }
                        }
                    }
                    "tools_query" => {
                        let query_id = data.get("id").and_then(|v| v.as_str())
                            .expect("tools_query missing id");
                        ws_emit(&ws_tx, "tools_response", "", "", json!({
                            "id": query_id,
                            "tools": [
                                { "who": "wicket", "f": "zsh", "description": "Execute a command in a sandboxed Zsh shell. Args: command (string), host (string, default localhost), run_in_background (bool), timeout (int ms), escalate (bool), reason (string)." },
                                { "who": "wicket", "f": "apply_patch", "description": "Apply a structured diff patch to files. Args: patch (string), host (string, default localhost)." },
                                { "who": "wicket", "f": "view_image", "description": "View an image file. Returns the image inline. Args: path (string), host (string, default localhost)." },
                                { "who": "wicket", "f": "shell", "description": "Execute an unsandboxed shell command. Args: command (string), host (string, default localhost)." }
                            ]
                        }));
                    }
                    "shutdown" => {
                        tracing::info!("shutdown requested");
                        break;
                    }
                    _ => {
                        tracing::debug!(stream = %stream, "ignoring unknown envelope");
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                tracing::warn!("websocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    tracing::info!("wicket shutting down");
}

fn gethostname() -> String {
    #[cfg(unix)]
    {
        use std::ffi::CStr;
        let mut buf = [0u8; 256];
        unsafe {
            if libc::gethostname(buf.as_mut_ptr() as *mut _, buf.len()) == 0 {
                return CStr::from_ptr(buf.as_ptr() as *const _)
                    .to_string_lossy()
                    .to_string();
            }
        }
    }
    "unknown".to_string()
}
