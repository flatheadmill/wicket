// Wicket: tool executor that connects back to Easement over WebSocket.
//
// Receives tool call envelopes (zsh, apply_patch, view_image, shell),
// executes them in a sandbox, returns results. One Wicket per host.
//
// Usage:
//   wicket ws://localhost:6502           # local executor
//   ssh host wicket ws://localhost:6502  # remote executor (via SSH tunnel)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
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

fn home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_default())
}

fn pane_dir(slug: &str) -> PathBuf {
    home_dir().join("pane").join(slug)
}

fn wicket_state_dir(slug: &str) -> PathBuf {
    home_dir().join(".local/state/wicket").join(slug)
}

fn job_dir(slug: &str, host_identity: &str) -> PathBuf {
    wicket_state_dir(slug).join("jobs").join(host_identity)
}

fn running_job_path(slug: &str, host_identity: &str, job_id: &str) -> PathBuf {
    job_dir(slug, host_identity).join(format!("{}.running.job", job_id))
}

fn finished_job_path(slug: &str, host_identity: &str, job_id: &str, exit_code: i32) -> PathBuf {
    job_dir(slug, host_identity).join(format!("{}.{}.job", job_id, exit_code))
}

async fn find_job_path(
    slug: &str,
    host_identity: &str,
    job_id: &str,
) -> std::io::Result<Option<PathBuf>> {
    let dir = job_dir(slug, host_identity);
    let running = dir.join(format!("{}.running.job", job_id));
    if tokio::fs::try_exists(&running).await? {
        return Ok(Some(running));
    }

    let prefix = format!("{}.", job_id);
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if file_name.starts_with(&prefix) && file_name.ends_with(".job") {
            return Ok(Some(entry.path()));
        }
    }

    Ok(None)
}

async fn ensure_pane_dir(slug: &str) -> std::io::Result<PathBuf> {
    let dir = pane_dir(slug);
    tokio::fs::create_dir_all(&dir).await?;
    Ok(dir)
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
                    let entry = LogEntry {
                        when: now(),
                        what: msg,
                    };
                    if let Ok(mut line) = serde_json::to_string(&entry) {
                        line.push('\n');
                        let _ = file.write_all(line.as_bytes()).await;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let shed = LogEntry {
                        when: now(),
                        what: LogMessage {
                            when: now(),
                            level: 0,
                            who: "log",
                            what: "lifecycle",
                            why: "shed",
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

type RunningJobs = Arc<Mutex<HashMap<String, RunningJob>>>;

#[derive(Clone, Serialize)]
struct RunningJobInfo {
    job_id: String,
    slug: String,
    transcript: String,
    r#where: String,
    command: String,
    output_path: String,
    started_at: String,
}

struct RunningJob {
    info: RunningJobInfo,
    kill_tx: Option<oneshot::Sender<()>>,
}

// The sandbox is the permission system. Every zsh command runs inside seatbelt (macOS)
// or bubblewrap (Linux) with deny-default, full read, and write only to paths listed in
// sandbox.conf. The config lives outside the sandbox so the executor cannot expand its
// own boundaries. Unsandboxed execution requires the escalation path through the operator.
struct SandboxConfig {
    writable: Vec<String>,
}

async fn read_sandbox_config(slug: &str) -> SandboxConfig {
    let home = std::env::var("HOME").unwrap_or_default();
    let path = wicket_state_dir(slug).join("sandbox.conf");

    let mut writable = vec![pane_dir(slug).to_string_lossy().to_string()];

    if let Ok(content) = tokio::fs::read_to_string(&path).await {
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
    policy.push_str(
        "(allow file-write-data (require-all (path \"/dev/null\") (vnode-type \
         CHARACTER-DEVICE)))\n",
    );
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
    cmd.arg("-p")
        .arg(&policy)
        .arg("--")
        .arg("zsh")
        .arg("-c")
        .arg(command);
    cmd
}

#[cfg(target_os = "linux")]
fn build_sandbox_command(command: &str, config: &SandboxConfig) -> tokio::process::Command {
    let mut args = vec![
        "--new-session".to_string(),
        "--die-with-parent".to_string(),
        "--ro-bind".to_string(),
        "/".to_string(),
        "/".to_string(),
        "--dev".to_string(),
        "/dev".to_string(),
        "--proc".to_string(),
        "/proc".to_string(),
        "--tmpfs".to_string(),
        "/tmp".to_string(),
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

async fn append_job_stream<R>(
    stream: R,
    file: Arc<Mutex<tokio::fs::File>>,
    tx: WsSender,
    job_id: String,
    output_path: PathBuf,
    is_localhost: bool,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                {
                    let mut file = file.lock().await;
                    let _ = file.write_all(line.as_bytes()).await;
                }
                if !is_localhost {
                    send(
                        &tx,
                        Outbound::Tool(ToolOutbound::BackgroundOutput {
                            job_id: job_id.clone(),
                            output_path: output_path.to_string_lossy().to_string(),
                            line: line.trim_end().to_string(),
                        }),
                    );
                }
            }
            Err(_) => break,
        }
    }
}

// Sandboxed shell. Runs inside seatbelt or bubblewrap. Supports foreground (wait for
// output) and background (spawn, return immediately, notify when done). Background tasks
// stream stdout to a file and emit a completion event — the CLI delivers it to Claude as
// a task notification on the next turn.
async fn handle_zsh(
    tx: &WsSender,
    jobs: &RunningJobs,
    slug: &str,
    call_id: &str,
    host_identity: &str,
    data: Value,
) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    let sandboxed = data
        .get("sandboxed")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let run_bg = data
        .get("run_in_background")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let transcript = data
        .get("transcript")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let job_id = data.get("job_id").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "zsh_exec", "command": command, "sandboxed": sandboxed, "run_bg": run_bg);

    let cwd = match ensure_pane_dir(slug).await {
        Ok(cwd) => cwd,
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("failed to create pane directory: {}", e),
                    exit_code: 1,
                }),
            );
            return;
        }
    };

    if run_bg {
        let output_dir = job_dir(slug, host_identity);
        let _ = tokio::fs::create_dir_all(&output_dir).await;
        let output_path = running_job_path(slug, host_identity, job_id);

        let file = match tokio::fs::File::create(&output_path).await {
            Ok(f) => f,
            Err(e) => {
                send(
                    tx,
                    Outbound::Tool(ToolOutbound::Response {
                        call_id: call_id.to_string(),
                        output: format!("failed to create output file: {}", e),
                        exit_code: 1,
                    }),
                );
                return;
            }
        };

        let mut cmd = if sandboxed {
            let config = read_sandbox_config(slug).await;
            let mut c = build_sandbox_command(command, &config);
            c.env("RUNNING_UNDER_WICKET", "1");
            c
        } else {
            let mut c = tokio::process::Command::new("zsh");
            c.env("RUNNING_UNDER_WICKET", "1");
            c.arg("-c").arg(command);
            c
        };
        cmd.current_dir(&cwd);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        match cmd.spawn() {
            Ok(mut child) => {
                let (kill_tx, mut kill_rx) = oneshot::channel();
                jobs.lock().await.insert(
                    job_id.to_string(),
                    RunningJob {
                        info: RunningJobInfo {
                            job_id: job_id.to_string(),
                            slug: slug.to_string(),
                            transcript: transcript.to_string(),
                            r#where: host_identity.to_string(),
                            command: command.to_string(),
                            output_path: output_path.to_string_lossy().to_string(),
                            started_at: now(),
                        },
                        kill_tx: Some(kill_tx),
                    },
                );

                send(
                    tx,
                    Outbound::Tool(ToolOutbound::Response {
                        call_id: call_id.to_string(),
                        output: format!(
                            "Background job {} started. Output: {}",
                            job_id,
                            output_path.display()
                        ),
                        exit_code: 0,
                    }),
                );

                let tx = tx.clone();
                let output_path = output_path.clone();
                let slug = slug.to_string();
                let transcript = transcript.to_string();
                let host_identity = host_identity.to_string();
                let job_id = job_id.to_string();
                let jobs = jobs.clone();
                let is_localhost = host_identity == "localhost";
                let file = Arc::new(Mutex::new(file));

                let child_stdout = child.stdout.take();
                let child_stderr = child.stderr.take();

                tokio::spawn(async move {
                    let mut stream_tasks = Vec::new();
                    if let Some(stdout) = child_stdout {
                        stream_tasks.push(tokio::spawn(append_job_stream(
                            stdout,
                            file.clone(),
                            tx.clone(),
                            job_id.clone(),
                            output_path.clone(),
                            is_localhost,
                        )));
                    }
                    if let Some(stderr) = child_stderr {
                        stream_tasks.push(tokio::spawn(append_job_stream(
                            stderr,
                            file.clone(),
                            tx.clone(),
                            job_id.clone(),
                            output_path.clone(),
                            is_localhost,
                        )));
                    }

                    let status = tokio::select! {
                        status = child.wait() => status,
                        _ = &mut kill_rx => {
                            trace!("wicket", "tool", "kill", "job_id": job_id);
                            if let Err(e) = child.start_kill() {
                                error!("wicket", "tool", "kill", e, "job_id": job_id);
                            }
                            child.wait().await
                        }
                    };
                    for task in stream_tasks {
                        let _ = task.await;
                    }
                    let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                    jobs.lock().await.remove(&job_id);
                    drop(file);
                    let finished_path = finished_job_path(&slug, &host_identity, &job_id, code);
                    let final_path = match tokio::fs::rename(&output_path, &finished_path).await {
                        Ok(()) => finished_path,
                        Err(e) => {
                            error!("wicket", "tool", "job_rename", e, "job_id": job_id);
                            output_path
                        }
                    };
                    let output_path_str = final_path.to_string_lossy().to_string();
                    let meta = format!(
                        "job_id={} where={} exit_code={} output_path={}",
                        job_id, host_identity, code, output_path_str
                    );
                    trace!("wicket", "tool", "notification", "job_id": job_id, "exit_code": code);
                    send(
                        &tx,
                        Outbound::Tool(ToolOutbound::Notification {
                            slug,
                            transcript,
                            message: "Background job exited.".to_string(),
                            meta: Some(meta),
                        }),
                    );
                });
            }
            Err(e) => {
                send(
                    tx,
                    Outbound::Tool(ToolOutbound::Response {
                        call_id: call_id.to_string(),
                        output: format!("failed to spawn background task: {}", e),
                        exit_code: 1,
                    }),
                );
            }
        }
        return;
    }

    let output = if sandboxed {
        let config = read_sandbox_config(slug).await;
        let mut cmd = build_sandbox_command(command, &config);
        cmd.env("RUNNING_UNDER_WICKET", "1");
        cmd.current_dir(&cwd);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        cmd.output().await
    } else {
        trace!("wicket", "tool", "unsandboxed", "command": command);
        let mut cmd = tokio::process::Command::new("zsh");
        cmd.env("RUNNING_UNDER_WICKET", "1");
        cmd.current_dir(&cwd);
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
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: combined,
                    exit_code: code,
                }),
            );
        }
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("failed to execute: {}", e),
                    exit_code: 1,
                }),
            );
        }
    }
}

// The operator's ! command. Unsandboxed, unadvertised. Exists in the dispatch but not
// in the tools manifest. Claude cannot discover or call it.
async fn handle_shell(tx: &WsSender, slug: &str, transcript: &str, call_id: &str, data: Value) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    trace!("wicket", "shell", "exec", "command": command);

    let cwd = match ensure_pane_dir(slug).await {
        Ok(cwd) => cwd,
        Err(e) => {
            send(
                tx,
                Outbound::Shell(ShellOutbound::Response {
                    id: call_id.to_string(),
                    slug: slug.to_string(),
                    transcript: transcript.to_string(),
                    output: format!("failed to create pane directory: {}", e),
                    exit_code: 1,
                }),
            );
            return;
        }
    };

    let output = tokio::process::Command::new("zsh")
        .env("RUNNING_UNDER_WICKET", "1")
        .current_dir(&cwd)
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
            send(
                tx,
                Outbound::Shell(ShellOutbound::Response {
                    id: call_id.to_string(),
                    slug: slug.to_string(),
                    transcript: transcript.to_string(),
                    output: combined,
                    exit_code: code,
                }),
            );
        }
        Err(e) => {
            send(
                tx,
                Outbound::Shell(ShellOutbound::Response {
                    id: call_id.to_string(),
                    slug: slug.to_string(),
                    transcript: transcript.to_string(),
                    output: format!("failed to execute: {}", e),
                    exit_code: 1,
                }),
            );
        }
    }
}

async fn handle_job(tx: &WsSender, slug: &str, call_id: &str, host_identity: &str, job_id: &str) {
    let path = match find_job_path(slug, host_identity, job_id).await {
        Ok(Some(path)) => path,
        Ok(None) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("job {} not found on {}", job_id, host_identity),
                    exit_code: 1,
                }),
            );
            return;
        }
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("failed to find job {}: {}", job_id, e),
                    exit_code: 1,
                }),
            );
            return;
        }
    };

    match tokio::fs::read_to_string(&path).await {
        Ok(output) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("job {} output at {}\n\n{}", job_id, path.display(), output),
                    exit_code: 0,
                }),
            );
        }
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("failed to read job {}: {}", job_id, e),
                    exit_code: 1,
                }),
            );
        }
    }
}

async fn handle_jobs(
    tx: &WsSender,
    jobs: &RunningJobs,
    slug: &str,
    transcript: &str,
    call_id: &str,
    host_identity: &str,
) {
    let running = jobs.lock().await;
    let jobs = running
        .values()
        .filter(|job| {
            job.info.slug == slug
                && job.info.transcript == transcript
                && job.info.r#where == host_identity
        })
        .map(|job| job.info.clone())
        .collect::<Vec<_>>();
    drop(running);

    let output = if jobs.is_empty() {
        format!(
            "no running jobs for slug {} transcript {} on {}",
            slug, transcript, host_identity
        )
    } else {
        serde_json::to_string_pretty(&jobs).unwrap_or_else(|_| "[]".to_string())
    };

    send(
        tx,
        Outbound::Tool(ToolOutbound::Response {
            call_id: call_id.to_string(),
            output,
            exit_code: 0,
        }),
    );
}

async fn handle_kill(
    tx: &WsSender,
    jobs: &RunningJobs,
    slug: &str,
    transcript: &str,
    call_id: &str,
    host_identity: &str,
    job_id: &str,
) {
    let mut running = jobs.lock().await;
    let Some(job) = running.get_mut(job_id) else {
        drop(running);
        send(
            tx,
            Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!(
                    "job {} is not running for slug {} transcript {} on {}",
                    job_id, slug, transcript, host_identity
                ),
                exit_code: 1,
            }),
        );
        return;
    };

    if job.info.slug != slug
        || job.info.transcript != transcript
        || job.info.r#where != host_identity
    {
        drop(running);
        send(
            tx,
            Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!(
                    "job {} is not running for slug {} transcript {} on {}",
                    job_id, slug, transcript, host_identity
                ),
                exit_code: 1,
            }),
        );
        return;
    }

    let Some(kill_tx) = job.kill_tx.take() else {
        drop(running);
        send(
            tx,
            Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("kill already requested for job {}", job_id),
                exit_code: 1,
            }),
        );
        return;
    };
    drop(running);

    let sent = kill_tx.send(()).is_ok();
    send(
        tx,
        Outbound::Tool(ToolOutbound::Response {
            call_id: call_id.to_string(),
            output: if sent {
                format!("kill requested for job {}", job_id)
            } else {
                format!("job {} finished before kill request was delivered", job_id)
            },
            exit_code: if sent { 0 } else { 1 },
        }),
    );
}

// Structured diffs via the codex-apply-patch crate. Validates every target path against
// the sandbox writable roots before applying — the kernel sandbox would catch violations
// too, but checking first gives a clear error instead of a cryptic seatbelt denial.
async fn handle_apply_patch(tx: &WsSender, slug: &str, call_id: &str, data: Value) {
    let patch = data.get("patch").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "apply_patch");

    let sandbox_config = read_sandbox_config(slug).await;
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
                let is_writable = sandbox_config
                    .writable
                    .iter()
                    .any(|root| abs.starts_with(root));
                if !is_writable {
                    denied_path = Some(abs);
                    break;
                }
            }

            if let Some(denied) = denied_path {
                send(
                    tx,
                    Outbound::Tool(ToolOutbound::Response {
                        call_id: call_id.to_string(),
                        output: format!("patch denied: {} is not inside a writable root", denied),
                        exit_code: 1,
                    }),
                );
            } else {
                let mut stdout_buf = Vec::new();
                let mut stderr_buf = Vec::new();
                match codex_apply_patch::apply_patch(patch, &mut stdout_buf, &mut stderr_buf) {
                    Ok(()) => {
                        let output = String::from_utf8_lossy(&stdout_buf);
                        send(
                            tx,
                            Outbound::Tool(ToolOutbound::Response {
                                call_id: call_id.to_string(),
                                output: output.trim_end().to_string(),
                                exit_code: 0,
                            }),
                        );
                    }
                    Err(e) => {
                        let stderr_str = String::from_utf8_lossy(&stderr_buf);
                        let output = if stderr_str.is_empty() {
                            format!("patch failed: {}", e)
                        } else {
                            format!("{}\npatch failed: {}", stderr_str.trim_end(), e)
                        };
                        send(
                            tx,
                            Outbound::Tool(ToolOutbound::Response {
                                call_id: call_id.to_string(),
                                output,
                                exit_code: 1,
                            }),
                        );
                    }
                }
            }
        }
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("patch parse error: {}", e),
                    exit_code: 1,
                }),
            );
        }
    }
}

fn resolve_tool_path(path_str: &str) -> PathBuf {
    let path = Path::new(path_str);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    }
}

// Read an image, resize to fit 1568px on the long edge (Anthropic's optimal threshold),
// base64 encode, return as an MCP image content block. Claude sees the image inline.
// Preserves source format when possible, falls back to JPEG on resize.
async fn handle_view_image(tx: &WsSender, call_id: &str, data: Value) {
    let path_str = data.get("path").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "view_image", "path": path_str);

    let abs_path = resolve_tool_path(path_str);

    let file_bytes = match tokio::fs::read(&abs_path).await {
        Ok(b) => b,
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("cannot read image: {}", e),
                    exit_code: 1,
                }),
            );
            return;
        }
    };

    let img = match image::load_from_memory(&file_bytes) {
        Ok(i) => i,
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("cannot decode image: {}", e),
                    exit_code: 1,
                }),
            );
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
        resized
            .write_to(&mut buf, image::ImageFormat::Jpeg)
            .unwrap_or_else(|e| error!("wicket", "tool", "jpeg_encode_failed", e));
        (buf.into_inner(), rw, rh, "image/jpeg")
    } else {
        let ext = abs_path
            .extension()
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

    send(
        tx,
        Outbound::Tool(ToolOutbound::Response {
            call_id: call_id.to_string(),
            output: serde_json::to_string(&content).unwrap_or_default(),
            exit_code: 0,
        }),
    );
}

// Read a PDF as an Anthropic document content block. Large or page-specific PDF
// handling belongs in a separate utility path; this tool just sends the PDF bytes.
async fn handle_read_pdf(tx: &WsSender, call_id: &str, data: Value) {
    let path_str = data.get("path").and_then(|v| v.as_str()).unwrap_or("");
    trace!("wicket", "tool", "read_pdf", "path": path_str);

    let abs_path = resolve_tool_path(path_str);
    let file_bytes = match tokio::fs::read(&abs_path).await {
        Ok(b) => b,
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!("cannot read PDF: {}", e),
                    exit_code: 1,
                }),
            );
            return;
        }
    };

    if file_bytes.is_empty() {
        send(
            tx,
            Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("PDF file is empty: {}", abs_path.display()),
                exit_code: 1,
            }),
        );
        return;
    }

    if !file_bytes.starts_with(b"%PDF-") {
        send(
            tx,
            Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!("file is not a valid PDF: {}", abs_path.display()),
                exit_code: 1,
            }),
        );
        return;
    }

    let page_count = match pdf_page_count(&abs_path).await {
        Ok(count) => count,
        Err(e) => {
            send(
                tx,
                Outbound::Tool(ToolOutbound::Response {
                    call_id: call_id.to_string(),
                    output: format!(
                        "cannot determine PDF page count for {}: {}. Use a PDF utility to split \
                         or render a page range first.",
                        abs_path.display(),
                        e
                    ),
                    exit_code: 1,
                }),
            );
            return;
        }
    };

    if page_count > 20 {
        send(
            tx,
            Outbound::Tool(ToolOutbound::Response {
                call_id: call_id.to_string(),
                output: format!(
                    "PDF has {} pages, which is too many to send at once. Use a PDF utility to \
                     split or render 20 pages or fewer, then read that smaller PDF.",
                    page_count
                ),
                exit_code: 1,
            }),
        );
        return;
    }

    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&file_bytes);
    let content = json!([
        { "type": "text", "text": format!("{} ({} pages, {} bytes application/pdf)", abs_path.display(), page_count, file_bytes.len()) },
        {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": encoded
            }
        }
    ]);

    send(
        tx,
        Outbound::Tool(ToolOutbound::Response {
            call_id: call_id.to_string(),
            output: serde_json::to_string(&content).unwrap_or_default(),
            exit_code: 0,
        }),
    );
}

async fn pdf_page_count(path: &Path) -> Result<u32, String> {
    let output = tokio::process::Command::new("pdfinfo")
        .arg(path)
        .output()
        .await
        .map_err(|e| format!("failed to run pdfinfo: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        return Err(if message.is_empty() {
            format!("pdfinfo exited with {}", output.status)
        } else {
            format!("pdfinfo exited with {}: {}", output.status, message)
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let Some(rest) = line.strip_prefix("Pages:") else {
            continue;
        };
        let count = rest
            .trim()
            .parse::<u32>()
            .map_err(|e| format!("invalid pdfinfo Pages value: {}", e))?;
        return Ok(count);
    }

    Err("pdfinfo output did not include a Pages field".to_string())
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
    Run {
        slug: String,
        transcript: String,
        call_id: String,
        #[serde(flatten)]
        tool: ToolCall,
    },
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
    ReadPdf {
        path: String,
        r#where: String,
    },
    Job {
        job_id: String,
        r#where: String,
    },
    Jobs {
        r#where: String,
    },
    Kill {
        job_id: String,
        r#where: String,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ShellInbound {
    Run {
        slug: String,
        transcript: String,
        id: String,
        command: String,
        r#where: String,
    },
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
    Connect {
        who: String,
        r#where: String,
        tools: Vec<ToolDef>,
    },
}

#[derive(serde::Serialize)]
struct ToolDef {
    f: String,
    description: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ToolOutbound {
    Response {
        call_id: String,
        output: String,
        exit_code: i32,
    },
    BackgroundOutput {
        job_id: String,
        output_path: String,
        line: String,
    },
    Notification {
        slug: String,
        transcript: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<String>,
    },
}

#[derive(serde::Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ShellOutbound {
    Response {
        id: String,
        slug: String,
        transcript: String,
        output: String,
        exit_code: i32,
    },
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
    let host_identity = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "localhost".to_string());

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
    let running_jobs: RunningJobs = Arc::new(Mutex::new(HashMap::new()));

    tokio::spawn(async move {
        while let Some(msg) = ws_rx.recv().await {
            if ws_sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
        let _ = ws_sink.close().await;
    });

    // Connect: identify ourselves and register tools.
    send(
        &ws_tx,
        Outbound::Socket(SocketOutbound::Connect {
            who: "wicket".to_string(),
            r#where: host_identity.clone(),
            tools: vec![
                ToolDef {
                    f: "zsh".to_string(),
                    description: "Execute a command in a sandboxed Zsh shell. The command runs \
                                  inside a deny-default sandbox with full read and restricted \
                                  write. Args: command (string, required), where (string, \
                                  required, the host identity e.g. \"localhost\"), \
                                  run_in_background (bool, default false, returns immediately \
                                  with a job ID and notifies on completion), timeout (int ms, \
                                  optional), escalate (bool, default false, requests operator \
                                  approval to run unsandboxed)."
                        .to_string(),
                },
                ToolDef {
                    f: "apply_patch".to_string(),
                    description: "Apply a structured diff patch to files. The patch format uses \
                                  markers: *** Begin Patch, *** End Patch, *** Add File: <path>, \
                                  *** Delete File: <path>, *** Update File: <path>. Update hunks \
                                  use unified diff format with @@ line markers, context lines \
                                  prefixed with space, removals with -, additions with +. Args: \
                                  patch (string, required — the full patch text), where (string, \
                                  required — the host identity)."
                        .to_string(),
                },
                ToolDef {
                    f: "view_image".to_string(),
                    description: "View an image file. Reads the file, resizes to fit within \
                                  1568px on the long edge if needed, and returns the image inline \
                                  as a content block. Supports JPEG, PNG, GIF, WebP. Args: path \
                                  (string, required — absolute or relative file path), where \
                                  (string, required — the host identity)."
                        .to_string(),
                },
                ToolDef {
                    f: "read_pdf".to_string(),
                    description: "Read a PDF file and return it inline as an Anthropic document \
                                  content block. This refuses PDFs over 20 pages; use a PDF \
                                  utility to split or render a smaller page range first. Args: \
                                  path (string, required — absolute or relative PDF path), where \
                                  (string, required — the host identity)."
                        .to_string(),
                },
                ToolDef {
                    f: "job".to_string(),
                    description: "Read a background job's saved output from this host. Args: \
                                  job_id (string, required — the background job ID returned by \
                                  zsh), where (string, required — the host identity)."
                        .to_string(),
                },
                ToolDef {
                    f: "jobs".to_string(),
                    description: "List currently running background jobs for this slug and \
                                  transcript on this host. Does not scan saved job files. Args: \
                                  where (string, required — the host identity)."
                        .to_string(),
                },
                ToolDef {
                    f: "kill".to_string(),
                    description: "Kill a currently running background job for this slug and \
                                  transcript on this host. Args: job_id (string, required — the \
                                  running background job ID), where (string, required — the host \
                                  identity)."
                        .to_string(),
                },
            ],
        }),
    );
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
                    Inbound::Tool(ToolInbound::Run {
                        slug,
                        transcript,
                        call_id,
                        tool,
                    }) => {
                        let tool_where = match &tool {
                            ToolCall::Zsh { r#where, .. } => r#where,
                            ToolCall::ApplyPatch { r#where, .. } => r#where,
                            ToolCall::ViewImage { r#where, .. } => r#where,
                            ToolCall::ReadPdf { r#where, .. } => r#where,
                            ToolCall::Job { r#where, .. } => r#where,
                            ToolCall::Jobs { r#where } => r#where,
                            ToolCall::Kill { r#where, .. } => r#where,
                        };
                        if tool_where != &host_identity {
                            continue;
                        }
                        trace!("wicket", "tool", "run", "call_id": call_id);

                        match tool {
                            ToolCall::Zsh {
                                command,
                                run_in_background,
                                timeout,
                                escalate,
                                ..
                            } => {
                                let zsh_data = json!({
                                    "command": command,
                                    "sandboxed": !escalate,
                                    "run_in_background": run_in_background,
                                    "timeout": timeout,
                                    "job_id": call_id,
                                    "transcript": transcript,
                                });
                                handle_zsh(
                                    &ws_tx,
                                    &running_jobs,
                                    &slug,
                                    &call_id,
                                    &host_identity,
                                    zsh_data,
                                )
                                .await;
                            }
                            ToolCall::ApplyPatch { patch, .. } => {
                                let patch_data = json!({ "patch": patch });
                                handle_apply_patch(&ws_tx, &slug, &call_id, patch_data).await;
                            }
                            ToolCall::ViewImage { path, .. } => {
                                let image_data = json!({ "path": path });
                                handle_view_image(&ws_tx, &call_id, image_data).await;
                            }
                            ToolCall::ReadPdf { path, .. } => {
                                let pdf_data = json!({ "path": path });
                                handle_read_pdf(&ws_tx, &call_id, pdf_data).await;
                            }
                            ToolCall::Job { job_id, .. } => {
                                handle_job(&ws_tx, &slug, &call_id, &host_identity, &job_id).await;
                            }
                            ToolCall::Jobs { .. } => {
                                handle_jobs(
                                    &ws_tx,
                                    &running_jobs,
                                    &slug,
                                    &transcript,
                                    &call_id,
                                    &host_identity,
                                )
                                .await;
                            }
                            ToolCall::Kill { job_id, .. } => {
                                handle_kill(
                                    &ws_tx,
                                    &running_jobs,
                                    &slug,
                                    &transcript,
                                    &call_id,
                                    &host_identity,
                                    &job_id,
                                )
                                .await;
                            }
                        }
                    }
                    Inbound::Shell(ShellInbound::Run {
                        slug,
                        transcript,
                        id,
                        command,
                        r#where,
                    }) => {
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
