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
use std::sync::OnceLock;

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Serialize)]
struct LogMessage {
    when: String,
    level: u8,
    who: &'static str,
    what: &'static str,
    why: &'static str,
    #[serde(flatten)]
    payload: Value,
}

#[derive(Serialize)]
struct LogEntry {
    when: String,
    what: LogMessage,
}

static LOG: OnceLock<broadcast::Sender<LogMessage>> = OnceLock::new();

fn log(level: u8, msg: LogMessage) {
    if let Some(tx) = LOG.get() {
        let _ = tx.send(LogMessage { level, ..msg });
    }
}

macro_rules! trace {
    ($who:expr, $what:expr, $why:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(0, LogMessage {
            when: now(), level: 0, who: $who, what: $what, why: $why,
            payload: serde_json::json!({ $($key: $val),* }),
        })
    };
}

macro_rules! wire {
    ($who:expr, $what:expr, $why:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(1, LogMessage {
            when: now(), level: 1, who: $who, what: $what, why: $why,
            payload: serde_json::json!({ $($key: $val),* }),
        })
    };
}

macro_rules! dump {
    ($who:expr, $what:expr, $why:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(2, LogMessage {
            when: now(), level: 2, who: $who, what: $what, why: $why,
            payload: serde_json::json!({ $($key: $val),* }),
        })
    };
}

macro_rules! error {
    ($who:expr, $what:expr, $how:expr, $error:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(0, LogMessage {
            when: now(), level: 0, who: $who, what: $what, why: "error",
            payload: serde_json::json!({ "how": $how, "error": $error.to_string() $(, $key: $val)* }),
        })
    };
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// Wicket is spawned with null stdio so logs write to a JSONL file. The broadcast
// channel sheds load if the sink falls behind and reports how many were dropped.
async fn init_log(host_identity: &str) {
    let home = std::env::var("HOME").expect("HOME not set");
    let log_dir = PathBuf::from(&home).join(".local/state/wicket");
    let _ = tokio::fs::create_dir_all(&log_dir).await;

    let log_path = log_dir.join(format!("{}.jsonl", host_identity));

    let (tx, _) = broadcast::channel::<LogMessage>(4096);
    let mut rx = tx.subscribe();
    LOG.set(tx).expect("log already initialized");

    tokio::spawn(async move {
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("cannot open log file {}: {}", log_path.display(), e);
                return;
            }
        };

        use tokio::io::AsyncWriteExt;
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    let entry = LogEntry { when: now(), what: msg };
                    if let Ok(mut line) = serde_json::to_string(&entry) {
                        line.push('\n');
                        let _ = file.write_all(line.as_bytes()).await;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let shed = LogEntry {
                        when: now(),
                        what: LogMessage {
                            when: now(), level: 0, who: "log", what: "lifecycle", why: "shed",
                            payload: json!({ "count": n }),
                        },
                    };
                    if let Ok(mut line) = serde_json::to_string(&shed) {
                        line.push('\n');
                        let _ = file.write_all(line.as_bytes()).await;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}


type WsSender = mpsc::UnboundedSender<String>;




// The sandbox is the permission system. Every zsh command runs inside seatbelt (macOS)
// or bubblewrap (Linux) with deny-default, full read, and write only to paths listed in
// sandbox.conf. The config lives outside the sandbox so the executor cannot expand its
// own boundaries. Unsandboxed execution requires the escalation path through the operator.
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


// Sandboxed shell. Runs inside seatbelt or bubblewrap. Supports foreground (wait for
// output) and background (spawn, return immediately, notify when done). Background tasks
// stream stdout to a file and emit a completion event — the CLI delivers it to Claude as
// a task notification on the next turn.
async fn handle_zsh(tx: &WsSender, slug: &str, call_id: &str, host_identity: &str, data: Value) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    let sandboxed = data.get("sandboxed").and_then(|v| v.as_bool()).unwrap_or(true);
    let run_bg = data.get("run_in_background").and_then(|v| v.as_bool()).unwrap_or(false);
    let transcript = data.get("transcript").and_then(|v| v.as_str()).unwrap_or("default");
    let task_uuid = data.get("task_uuid").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "zsh_exec", "command": command, "sandboxed": sandboxed, "run_bg": run_bg);

    if run_bg {
        let home = std::env::var("HOME").unwrap_or_default();
        let output_dir = Path::new(&home)
            .join(".local/state/wicket")
            .join(slug)
            .join(transcript)
            .join(host_identity);
        let _ = std::fs::create_dir_all(&output_dir);
        let output_path = output_dir.join(format!("{}.txt", task_uuid));

        let _file = match std::fs::File::create(&output_path) {
            Ok(f) => f,
            Err(e) => {
                send(tx, Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("failed to create output file: {}", e),
                    exit_code: 1,
                }));
                return;
            }
        };



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
                send(tx, Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("Background task {} started. Output: {}", task_uuid, output_path.display()),
                    exit_code: 0,
                }));

                let tx = tx.clone();
                let output_path = output_path.clone();
                let task_uuid = task_uuid.to_string();
                let is_localhost = host_identity == "localhost";

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
                                        send(&tx, Outbound::Tool(ToolOutbound::BackgroundOutput {
                                            task_uuid: task_uuid_clone.clone(),
                                            output_path: output_path.to_string_lossy().to_string(),
                                            line: line.trim_end().to_string(),
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
                    trace!("wicket", "tool", "background_done", "task_uuid": task_uuid, "exit_code": code);
                    send(&tx, Outbound::Tool(ToolOutbound::BackgroundDone {
                        task_uuid,
                        exit_code: code,
                        output_path: output_path_str,
                    }));
                });
            }
            Err(e) => {
                send(tx, Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("failed to spawn background task: {}", e),
                    exit_code: 1,
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
        trace!("wicket", "tool", "unsandboxed", "command": command);
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
            send(tx, Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: combined,
                exit_code: code,
            }));
        }
        Err(e) => {
            send(tx, Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("failed to execute: {}", e),
                exit_code: 1,
            }));
        }
    }
}

// The operator's ! command. Unsandboxed, unadvertised. Exists in the dispatch but not
// in the tools manifest. Claude cannot discover or call it.
async fn handle_shell(tx: &WsSender, slug: &str, transcript: &str, call_id: &str, data: Value) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    trace!("wicket", "shell", "exec", "command": command);

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
            send(tx, Outbound::Shell(ShellOutbound::Response {
                id: call_id.to_string(),
                slug: slug.to_string(),
                transcript: transcript.to_string(),
                output: combined,
                exit_code: code,
            }));
        }
        Err(e) => {
            send(tx, Outbound::Shell(ShellOutbound::Response {
                id: call_id.to_string(),
                slug: slug.to_string(),
                transcript: transcript.to_string(),
                output: format!("failed to execute: {}", e),
                exit_code: 1,
            }));
        }
    }
}

// Structured diffs via the codex-apply-patch crate. Validates every target path against
// the sandbox writable roots before applying — the kernel sandbox would catch violations
// too, but checking first gives a clear error instead of a cryptic seatbelt denial.
async fn handle_apply_patch(tx: &WsSender, slug: &str, call_id: &str, data: Value) {
    let patch = data.get("patch").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "apply_patch");

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
                send(tx, Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("patch denied: {} is not inside a writable root", denied),
                    exit_code: 1,
                }));
            } else {
                let mut stdout_buf = Vec::new();
                let mut stderr_buf = Vec::new();
                match codex_apply_patch::apply_patch(patch, &mut stdout_buf, &mut stderr_buf) {
                    Ok(()) => {
                        let output = String::from_utf8_lossy(&stdout_buf);
                        send(tx, Outbound::Tool(ToolOutbound::Response {
                            call_id: call_id.to_string(),
                            output: output.trim_end().to_string(),
                            exit_code: 0,
                        }));
                    }
                    Err(e) => {
                        let stderr_str = String::from_utf8_lossy(&stderr_buf);
                        let output = if stderr_str.is_empty() {
                            format!("patch failed: {}", e)
                        } else {
                            format!("{}\npatch failed: {}", stderr_str.trim_end(), e)
                        };
                        send(tx, Outbound::Tool(ToolOutbound::Response {
                            call_id: call_id.to_string(),
                            output,
                            exit_code: 1,
                        }));
                    }
                }
            }
        }
        Err(e) => {
            send(tx, Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("patch parse error: {}", e),
                exit_code: 1,
            }));
        }
    }
}

// Read an image, resize to fit 1568px on the long edge (Anthropic's optimal threshold),
// base64 encode, return as an MCP image content block. Claude sees the image inline.
// Preserves source format when possible, falls back to JPEG on resize.
async fn handle_view_image(tx: &WsSender, call_id: &str, data: Value) {
    let path_str = data.get("path").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "view_image", "path": path_str);

    let path = Path::new(path_str);
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };

    let file_bytes = match std::fs::read(&abs_path) {
        Ok(b) => b,
        Err(e) => {
            send(tx, Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("cannot read image: {}", e),
                exit_code: 1,
            }));
            return;
        }
    };

    let img = match image::load_from_memory(&file_bytes) {
        Ok(i) => i,
        Err(e) => {
            send(tx, Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("cannot decode image: {}", e),
                exit_code: 1,
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
            .unwrap_or_else(|e| error!("wicket", "tool", "jpeg_encode_failed", e));
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

    trace!("wicket", "tool", "view_image_encoded", "original": format!("{}x{}", w, h), "output": format!("{}x{}", output_w, output_h), "resized": needs_resize, "bytes": output_bytes.len());

    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&output_bytes);

    let content = json!([
        { "type": "text", "text": format!("{} ({}x{} {})", abs_path.display(), output_w, output_h, media_type) },
        { "type": "image", "data": encoded, "mimeType": media_type }
    ]);

    send(tx, Outbound::Tool(ToolOutbound::Response {
        call_id: call_id.to_string(),
        output: serde_json::to_string(&content).unwrap_or_default(),
        exit_code: 0,
    }));
}


// The envelope protocol between Easement and Wicket is strict. Every field is
// required. No Option unless the absence is a real state (e.g. timeout not
// set). A missing field is a serialization bug, not a condition to handle at
// runtime.
//
// The ToolCall variants are the exception. Their fields are function arguments
// that Claude fills in through tool descriptions. Additive booleans like
// run_in_background and escalate default to false via #[serde(default)]
// because the caller only mentions them when opting in. This is a user
// interface, not a wire protocol.

#[derive(serde::Deserialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Inbound {
    Tool(ToolInbound),
    Shell(ShellInbound),
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ToolInbound {
    Run { slug: String, transcript: String, call_id: String, #[serde(flatten)] tool: ToolCall },
}

#[derive(serde::Deserialize)]
#[serde(tag = "f", rename_all = "snake_case")]
enum ToolCall {
    Zsh {
        command: String,
        r#where: String,
        #[serde(default)]
        run_in_background: bool,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        escalate: bool,
    },
    ApplyPatch {
        patch: String,
        r#where: String,
    },
    ViewImage {
        path: String,
        r#where: String,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ShellInbound {
    Run { slug: String, transcript: String, id: String, command: String, r#where: String },
}

// Outbound: what Wicket sends to Easement.
#[derive(serde::Serialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Outbound {
    Socket(SocketOutbound),
    Tool(ToolOutbound),
    Shell(ShellOutbound),
}

#[derive(serde::Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum SocketOutbound {
    Connect { who: String, r#where: String, tools: Vec<ToolDef> },
}

#[derive(serde::Serialize)]
struct ToolDef {
    f: String,
    description: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ToolOutbound {
    Response { call_id: String, output: String, exit_code: i32 },
    BackgroundOutput { task_uuid: String, output_path: String, line: String },
    BackgroundDone { task_uuid: String, exit_code: i32, output_path: String },
}

#[derive(serde::Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ShellOutbound {
    Response { id: String, slug: String, transcript: String, output: String, exit_code: i32 },
}

fn send(tx: &WsSender, msg: Outbound) {
    match serde_json::to_string(&msg) {
        Ok(json) => {
            wire!("wicket", "websocket", "send", "raw": json);
            let _ = tx.send(json);
        }
        Err(e) => error!("wicket", "websocket", "serialize", e),
    }
}

#[tokio::main]
async fn main() {

    let args: Vec<String> = std::env::args().collect();
    let wicket_url = args.get(1).cloned().unwrap_or_else(|| {
        eprintln!("usage: wicket <ws-url> <host-identity>");
        std::process::exit(1);
    });
    let host_identity = args.get(2).cloned().unwrap_or_else(|| "localhost".to_string());

    init_log(&host_identity).await;
    trace!("wicket", "lifecycle", "starting", "url": wicket_url, "where": host_identity);

    let (ws_stream, _) = match tokio_tungstenite::connect_async(&wicket_url).await {
        Ok(s) => s,
        Err(e) => {
            error!("wicket", "lifecycle", "connect_failed", e);
            eprintln!("cannot connect to easement: {}", e);
            std::process::exit(1);
        }
    };

    let (mut ws_sink, mut ws_stream_rx) = ws_stream.split();

    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        while let Some(msg) = ws_rx.recv().await {
            if ws_sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
        let _ = ws_sink.close().await;
    });

    // Connect: identify ourselves and register tools.
    send(&ws_tx, Outbound::Socket(SocketOutbound::Connect {
        who: "wicket".to_string(),
        r#where: host_identity.clone(),
        tools: vec![
            ToolDef {
                f: "zsh".to_string(),
                description: "Execute a command in a sandboxed Zsh shell. The command runs inside a deny-default sandbox with full read and restricted write. Args: command (string, required), where (string, required, the host identity e.g. \"localhost\"), run_in_background (bool, default false, returns immediately with a task ID and notifies on completion), timeout (int ms, optional), escalate (bool, default false, requests operator approval to run unsandboxed).".to_string(),
            },
            ToolDef {
                f: "apply_patch".to_string(),
                description: "Apply a structured diff patch to files. The patch format uses markers: *** Begin Patch, *** End Patch, *** Add File: <path>, *** Delete File: <path>, *** Update File: <path>. Update hunks use unified diff format with @@ line markers, context lines prefixed with space, removals with -, additions with +. Args: patch (string, required — the full patch text), where (string, required — the host identity).".to_string(),
            },
            ToolDef {
                f: "view_image".to_string(),
                description: "View an image file. Reads the file, resizes to fit within 1568px on the long edge if needed, and returns the image inline as a content block. Supports JPEG, PNG, GIF, WebP. Args: path (string, required — absolute or relative file path), where (string, required — the host identity).".to_string(),
            },
        ],
    }));
    trace!("wicket", "lifecycle", "connected");

    while let Some(result) = ws_stream_rx.next().await {
        match result {
            Ok(Message::Text(text)) => {
                let raw: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("wicket", "websocket", "parse_json", e, "raw": text);
                        continue;
                    }
                };

                let what = raw.get("what").and_then(|v| v.as_str()).unwrap_or("");
                if what == "tool" || what == "shell" {
                    wire!("wicket", "websocket", "recv", "raw": raw);
                } else {
                    dump!("wicket", "websocket", "ignored", "raw": raw);
                    continue;
                }

                let msg: Inbound = match serde_json::from_value(raw.clone()) {
                    Ok(m) => m,
                    Err(e) => {
                        error!("wicket", "websocket", "decode", e, "raw": raw);
                        continue;
                    }
                };

                match msg {
                    Inbound::Tool(ToolInbound::Run { slug, transcript, call_id, tool }) => {
                        let tool_where = match &tool {
                            ToolCall::Zsh { r#where, .. } => r#where,
                            ToolCall::ApplyPatch { r#where, .. } => r#where,
                            ToolCall::ViewImage { r#where, .. } => r#where,
                        };
                        if tool_where != &host_identity {
                            continue;
                        }
                        trace!("wicket", "tool", "run", "call_id": call_id);

                        match tool {
                            ToolCall::Zsh { command, run_in_background, timeout, escalate, .. } => {
                                let zsh_data = json!({
                                    "command": command,
                                    "sandboxed": !escalate,
                                    "run_in_background": run_in_background,
                                    "timeout": timeout,
                                    "task_uuid": call_id,
                                    "transcript": transcript,
                                });
                                handle_zsh(&ws_tx, &slug, &call_id, &host_identity, zsh_data).await;
                            }
                            ToolCall::ApplyPatch { patch, .. } => {
                                let patch_data = json!({ "patch": patch });
                                handle_apply_patch(&ws_tx, &slug, &call_id, patch_data).await;
                            }
                            ToolCall::ViewImage { path, .. } => {
                                let image_data = json!({ "path": path });
                                handle_view_image(&ws_tx, &call_id, image_data).await;
                            }
                        }
                    }
                    Inbound::Shell(ShellInbound::Run { slug, transcript, id, command, r#where }) => {
                        if r#where != host_identity {
                            continue;
                        }
                        trace!("wicket", "shell", "run", "id": id, "command": command);
                        let shell_data = json!({ "command": command });
                        handle_shell(&ws_tx, &slug, &transcript, &id, shell_data).await;
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                error!("wicket", "websocket", "read", e);
                break;
            }
            _ => {}
        }
    }

    trace!("wicket", "lifecycle", "shutdown");
}
