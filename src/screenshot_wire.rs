//! Private screenshot wire adapter. Easement supplies the binding; the worker
//! owns all Store calls and live tickets. Ready/ack are temporary receive state.
//! No screenshot command or reply goes through the raw-wire logger.

use std::collections::HashMap;
use std::path::PathBuf;

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::screenshot::{
    Attempt, Intent, OperationId, Outcome, Status, Store, Ticket, MAX_CHUNK_BYTES,
};

pub const MAX_PACKET_BYTES: usize = 96 * 1024;
pub const QUEUE_CAPACITY: usize = 8;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub operation: OperationId,
    pub attempt: Attempt,
    pub source_socket: u64,
    pub writer_socket: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    what: String,
    why: String,
    binding: Binding,
    sequence: u64,
    #[serde(default)]
    intent: Option<Intent>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    data: Option<String>,
}

impl Request {
    /// Errors deliberately contain no parser diagnostics or supplied fields.
    pub fn parse(text: &str) -> Result<Self, &'static str> {
        if text.len() > MAX_PACKET_BYTES {
            return Err("screenshot packet exceeds limit");
        }
        let request: Self = serde_json::from_str(text).map_err(|_| "invalid screenshot command")?;
        let binding = &request.binding;
        if request.what != "screenshot_save"
            || binding.operation.caller != "shotgun"
            || [
                &binding.operation.slug,
                &binding.operation.operation_id,
                &binding.attempt.call_id,
                &binding.attempt.attempt_id,
            ]
            .iter()
            .any(|s| {
                s.is_empty()
                    || s.len() > 128
                    || !s
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
            })
        {
            return Err("invalid screenshot binding");
        }
        match request.why.as_str() {
            "begin"
                if request.intent.is_some()
                    && request.offset.is_none()
                    && request.data.is_none() => {}
            "chunk"
                if request.intent.is_none()
                    && request.offset.is_some()
                    && request
                        .data
                        .as_ref()
                        .is_some_and(|d| d.len() <= MAX_CHUNK_BYTES.div_ceil(3) * 4) => {}
            "finish" | "cancel" | "status"
                if request.intent.is_none()
                    && request.offset.is_none()
                    && request.data.is_none() => {}
            _ => return Err("invalid screenshot command fields"),
        }
        Ok(request)
    }

    pub fn unavailable(&self, reason: &str) -> Value {
        reply(self, "unresolved", Some(reason), None)
    }
}

pub struct Completion {
    pub packet: Value,
    pub observations: Vec<Value>,
}

pub struct Worker {
    sender: mpsc::Sender<Request>,
    task: JoinHandle<()>,
}

impl Worker {
    /// Starting the owner performs no filesystem access. Opening Store is lazy
    /// and failures remain screenshot capability failures, not Wicket startup failures.
    pub fn start(home: PathBuf, output: mpsc::Sender<Completion>) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Request>(QUEUE_CAPACITY);
        let task = tokio::task::spawn_blocking(move || {
            let mut adapter = Adapter {
                home,
                store: None,
                live: HashMap::new(),
            };
            while let Some(request) = receiver.blocking_recv() {
                let packet = adapter.handle(&request);
                let observations = adapter
                    .store
                    .as_mut()
                    .map(Store::take_observations)
                    .unwrap_or_default();
                if output
                    .blocking_send(Completion {
                        packet,
                        observations,
                    })
                    .is_err()
                {
                    break;
                }
            }
            // Dropping Store cleans temporary receiving only. Already queued
            // finish commands may publish before EOF; their journal survives.
        });
        Self { sender, task }
    }

    pub fn submit(&self, request: Request) -> Result<(), Box<Request>> {
        self.sender
            .try_send(request)
            .map_err(|e| Box::new(e.into_inner()))
    }

    /// Close the completion receiver first so a lost router cannot deadlock a
    /// blocking output send. Joining waits for filesystem work already in progress.
    pub async fn shutdown(self) {
        drop(self.sender);
        let _ = self.task.await;
    }
}

struct Live {
    binding: Binding,
    intent: Intent,
    ticket: Ticket,
    sequence: u64,
}

struct Adapter {
    home: PathBuf,
    store: Option<Store>,
    live: HashMap<(String, String), Live>,
}

impl Adapter {
    fn handle(&mut self, request: &Request) -> Value {
        if self.store.is_none() {
            match Store::open(&self.home) {
                Ok(store) => self.store = Some(store),
                Err(_) => {
                    return request.unavailable(
                        "screenshot store unavailable; operation requires reconciliation",
                    )
                }
            }
        }
        let operation = &request.binding.operation;
        let key = (operation.slug.clone(), operation.operation_id.clone());
        if request.why == "status" {
            let mut status = self.store.as_mut().expect("opened above").lookup(operation);
            if matches!(status, Ok(Some(Status::Unresolved { .. }))) && self.live.is_empty() {
                // A journal barrier failure freezes the writer. Reopen at an
                // explicit lookup boundary once no live tickets would be lost;
                // startup reconciliation can then recover a landed terminal
                // record or prove publication from its retained inode.
                drop(self.store.take());
                match Store::open(&self.home) {
                    Ok(mut store) => {
                        status = store.lookup(operation);
                        self.store = Some(store);
                    }
                    Err(_) => {
                        return request
                            .unavailable("screenshot store could not reopen for reconciliation")
                    }
                }
            }
            return match status {
                Ok(Some(status)) => status_reply(request, status, "receiving"),
                Ok(None) => reply(request, "unknown", None, None),
                Err(_) => request.unavailable("operation lookup unavailable"),
            };
        }
        let store = self.store.as_mut().expect("opened above");
        if request.why == "begin" {
            let intent = request.intent.as_ref().expect("validated command");
            let status = if let Some(live) = self.live.get(&key) {
                if live.binding == request.binding {
                    if request.sequence <= live.sequence {
                        return reply(request, "rejected", Some("stale command sequence"), None);
                    }
                    store.begin(
                        operation.clone(),
                        request.binding.attempt.clone(),
                        intent.clone(),
                    )
                } else if live.intent == *intent
                    && live.binding.operation == *operation
                    && live.binding.attempt.attempt_id != request.binding.attempt.attempt_id
                {
                    match store.replace(&live.ticket, request.binding.attempt.clone()) {
                        Ok(ticket) => Ok(Status::Receiving {
                            ticket,
                            received: 0,
                        }),
                        // Router has retired the old source binding. If a
                        // receive cannot restart, fence it rather than strand
                        // temporary staging with no source allowed to finish.
                        Err(_) => store.cancel(operation, &live.ticket),
                    }
                } else {
                    return reply(
                        request,
                        "rejected",
                        Some("operation intent or caller conflicts"),
                        None,
                    );
                }
            } else {
                store.begin(
                    operation.clone(),
                    request.binding.attempt.clone(),
                    intent.clone(),
                )
            };
            return match status {
                Ok(status) => {
                    if let Status::Receiving { ticket, .. } = &status {
                        self.live.insert(
                            key,
                            Live {
                                binding: request.binding.clone(),
                                intent: intent.clone(),
                                ticket: ticket.clone(),
                                sequence: request.sequence,
                            },
                        );
                    } else {
                        self.live.remove(&key);
                    }
                    status_reply(request, status, "ready")
                }
                Err(_) => reply(
                    request,
                    "rejected",
                    Some("screenshot begin refused; no receive was admitted for this binding"),
                    None,
                ),
            };
        }
        let Some(live) = self.live.get_mut(&key) else {
            if request.why == "finish" || request.why == "cancel" {
                if let Ok(Some(status @ (Status::Terminal(_) | Status::Unresolved { .. }))) =
                    store.lookup(operation)
                {
                    return status_reply(request, status, "receiving");
                }
            }
            return request.unavailable("no live attempt; query status with the operation ID");
        };
        if live.binding != request.binding || request.sequence <= live.sequence {
            return reply(
                request,
                "rejected",
                Some("stale or foreign attempt binding"),
                None,
            );
        }
        live.sequence = request.sequence;
        let result = match request.why.as_str() {
            "chunk" => {
                let data = request.data.as_ref().expect("validated chunk");
                match base64::engine::general_purpose::STANDARD.decode(data) {
                    Ok(bytes) if !bytes.is_empty() && bytes.len() <= MAX_CHUNK_BYTES => {
                        let offset = request.offset.expect("validated chunk");
                        if store.append(&live.ticket, offset, &bytes).is_ok() {
                            return reply(request, "ack", None, Some(offset + bytes.len() as u64));
                        }
                    }
                    _ => {}
                }
                // Fence the live receive before describing a malformed chunk
                // as aborted; never include the supplied data in the error.
                store.cancel(operation, &live.ticket)
            }
            "finish" => store.finish(operation, &live.ticket),
            "cancel" => store.cancel(operation, &live.ticket),
            _ => unreachable!("validated command"),
        };
        if result.is_ok() {
            self.live.remove(&key);
        }
        match result {
            Ok(status) => status_reply(request, status, "receiving"),
            Err(_) => request
                .unavailable("screenshot command could not establish an outcome; query status"),
        }
    }
}

fn reply(request: &Request, why: &str, reason: Option<&str>, offset: Option<u64>) -> Value {
    let mut packet = json!({ "what": "screenshot_save", "why": why,
        "binding": request.binding, "sequence": request.sequence });
    if let Some(reason) = reason {
        packet["reason"] = json!(reason);
    }
    if let Some(offset) = offset {
        packet["next_offset"] = json!(offset);
    }
    packet
}

fn status_reply(request: &Request, status: Status, receiving: &str) -> Value {
    match status {
        Status::Receiving { received, .. } => reply(request, receiving, None, Some(received)),
        Status::Unresolved { reason, .. } => reply(request, "unresolved", Some(&reason), None),
        Status::Terminal(receipt) => {
            let why = match receipt.outcome {
                Outcome::Stored => "stored",
                Outcome::PublishedDurabilityUnconfirmed { .. } => "published",
                Outcome::Failed { .. } => "failed",
                Outcome::Aborted => "aborted",
            };
            let mut packet = reply(request, why, None, None);
            packet["receipt"] =
                serde_json::to_value(receipt).expect("Store verified serializable receipt");
            packet
        }
    }
}

#[cfg(test)]
mod tests;
