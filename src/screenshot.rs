//! Screenshot-save core. All methods perform blocking I/O and belong on one
//! blocking worker, not the async router. `Store::open` is a fallible, lazy
//! capability boundary; it must not gate unrelated Wicket tools at startup.
//!
//! Receiving is process-local. Durable acceptance occurs only after full PNG
//! verification, stage synchronization, and journal synchronization. A retained
//! staging inode proves publication across a lost terminal write. Missing or
//! conflicting evidence remains unresolved; recovery never republishes.
//!
//! `begin` / `append` hold a temporary receive; `replace` fences its ticket and
//! restarts the bytes. `finish` verifies, synchronizes the stage and its parent,
//! persists acceptance, links without overwrite, then persists the terminal
//! receipt before returning it. `lookup` needs no ticket and can recover that
//! receipt after reconnect. IDs and caller identity must come from trusted
//! routing context; the caller should retain the operation ID before capture.
//!
//! Pre-acceptance failure/cancellation receipts have `accepted_at: None` and
//! are remembered only by this owner. After a process crash, a receive may
//! leave uniquely named staging litter and lookup returns None; neither implies
//! publication or an accepted transaction. Accepted records and receipts are
//! retained indefinitely. An unresolved operation emits no terminal observation.
//! Observations are best effort, not a durable outbox: a crash can lose a log
//! line even when the transaction receipt is recoverable.
//!
//! The core assumes a local filesystem and trusted local configuration/staging
//! namespace. Descriptor-relative operations prevent following substituted
//! symlinks; they cannot stop a hostile same-user process moving directories or
//! altering files between syscalls. Historical receipts do not promise that a
//! pathname remains unchanged after publication.

use std::collections::HashMap;
use std::ffi::{CString, OsStr};
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const MAX_PNG_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_TRANSFERS: usize = 4;
pub const MAX_RECEIVING_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_DIMENSION: u32 = 32_768;
pub const MAX_PIXELS: u64 = 32 * 1024 * 1024;
const MAX_DECODE_BYTES: usize = 128 * 1024 * 1024;
const MAX_JOURNAL_BYTES: u64 = 64 * 1024;
// The terminal record embeds the intent twice. Bound serialized metadata as
// well as strings so escaping cannot make an accepted operation unrecordable.
const MAX_INTENT_JSON_BYTES: usize = 16 * 1024;
const MAX_CANONICAL_PATH_JSON_BYTES: usize = 8 * 1024;
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    Viewport,
    FullPage,
}

/// Identity supplied by the trusted caller/router, never copied from a chunk.
/// An operation ID is scoped by slug; changing caller under that ID conflicts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationId {
    pub slug: String,
    pub caller: String,
    pub operation_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub call_id: String,
    pub attempt_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub destination: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    pub mode: CaptureMode,
    pub source_url: String,
    pub captured_at: String,
}

/// Only fields independently verified by Wicket live here. Capture URL, mode,
/// and time remain sender claims under Receipt::requested.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedPng {
    pub mime_type: String,
    pub bytes: u64,
    pub sha256: String,
    pub pixel_width: u32,
    pub pixel_height: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Stored,
    PublishedDurabilityUnconfirmed { reason: String },
    Failed { action: String, reason: String },
    Aborted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub operation: OperationId,
    pub attempt: Attempt,
    pub event_id: String,
    pub requested: Intent,
    pub destination: PathBuf,
    pub verified: Option<VerifiedPng>,
    /// Stage-ready observation time, immediately before the acceptance journal
    /// barrier; not a publication or commit timestamp. None for a process-local
    /// pre-acceptance failure or cancellation.
    pub accepted_at: Option<String>,
    /// Time the outcome was observed, possibly during recovery; not linkat time.
    pub observed_at: String,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Status {
    Receiving {
        ticket: Ticket,
        received: u64,
    },
    Terminal(Box<Receipt>),
    Unresolved {
        operation: OperationId,
        destination: PathBuf,
        /// Some(true) only when this owner observed linkat succeeding; None
        /// after restart without sufficient publication evidence.
        published: Option<bool>,
        reason: String,
    },
}

/// Opaque process-local authority. The wire owner retains this value and binds
/// packets to it. Replacement/reopen invalidates old tickets, even at offset 0.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ticket {
    key: String,
    owner: u64,
    generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Identity {
    device: u64,
    inode: u64,
}
impl Identity {
    fn of(file: &File) -> io::Result<Self> {
        let m = file.metadata()?;
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    operation: OperationId,
    attempt: Attempt,
    intent: Intent,
    destination: PathBuf,
    parent_identity: Identity,
    stage_name: String,
    stage_identity: Identity,
    verified: VerifiedPng,
    accepted_at: String,
    terminal: Option<Receipt>,
}

struct Receiving {
    operation: OperationId,
    attempt: Attempt,
    intent: Intent,
    destination: PathBuf,
    parent: File,
    stage: File,
    stage_name: String,
    ticket: Ticket,
}

pub struct Store {
    home: PathBuf,
    directory: File,
    _lock: File,
    owner: u64,
    next_generation: u64,
    receiving: HashMap<String, Receiving>,
    records: HashMap<String, Record>,
    // These outcomes bind IDs only for this process lifetime. They are not
    // accepted transactions and disappear on restart, as receiving state does.
    temporary_outcomes: HashMap<String, Receipt>,
    observations: Vec<Value>,
    poisoned: Option<String>,
    known_publication: HashMap<String, bool>,
    #[cfg(test)]
    fault: Option<Fault>,
}

impl Store {
    /// Call lazily from the screenshot worker. Failure disables only that
    /// capability. The lock covers loading, reconciliation and all mutations.
    pub fn open(home: &Path) -> io::Result<Self> {
        let home = fs::canonicalize(home)?;
        let mut directory = open_directory(&home)?;
        for name in [".local", "state", "wicket", "screenshots"] {
            mkdir_at(&directory, name)?;
            directory = open_at(
                &directory,
                name.as_ref(),
                libc::O_RDONLY | libc::O_DIRECTORY,
            )?;
        }
        directory.set_permissions(fs::Permissions::from_mode(0o700))?;
        let lock = open_at(&directory, ".lock".as_ref(), libc::O_RDWR | libc::O_CREAT)?;
        if !lock.metadata()?.is_file() || lock.metadata()?.nlink() != 1 {
            return Err(invalid("invalid screenshot store lock"));
        }
        // SAFETY: the live descriptor is owned for the Store's entire lifetime.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut store = Self {
            home,
            directory,
            _lock: lock,
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            next_generation: 0,
            receiving: HashMap::new(),
            records: HashMap::new(),
            temporary_outcomes: HashMap::new(),
            observations: Vec::new(),
            poisoned: None,
            known_publication: HashMap::new(),
            #[cfg(test)]
            fault: None,
        };
        for child in fs::read_dir(store.home.join(".local/state/wicket/screenshots"))? {
            let name = child?.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 journal name"))?;
            if name == ".lock" {
                continue;
            }
            if let Some(key) = name.strip_suffix(".new") {
                validate_key(key)?;
                unlink_at(&store.directory, name.as_ref())?;
                continue;
            }
            let key = name
                .strip_suffix(".json")
                .ok_or_else(|| invalid("unknown journal file"))?;
            validate_key(key)?;
            let file = open_at(&store.directory, name.as_ref(), libc::O_RDONLY)?;
            let record: Record =
                serde_json::from_slice(&read_bounded(file, MAX_JOURNAL_BYTES)?).map_err(invalid)?;
            validate_record(key, &record)?;
            store.records.insert(key.to_owned(), record);
        }
        // A previous owner may have renamed a synchronized record and lost
        // the directory barrier. Complete it before returning any receipt or
        // removing the stage that could still be needed as publication proof.
        store.directory.sync_all()?;
        let keys: Vec<_> = store.records.keys().cloned().collect();
        for key in keys {
            if store.records[&key].terminal.is_some() {
                let record = store.records[&key].clone();
                store.cleanup_record(&record);
            } else {
                store.reconcile(&key);
            }
        }
        Ok(store)
    }

    /// Drain pure observational records for the adapter's best-effort sink.
    /// Lookup/retry of an existing receipt does not emit another store fact.
    /// Accepted/chunk/progress traffic never enters this queue.
    pub fn take_observations(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.observations)
    }

    pub fn lookup(&mut self, operation: &OperationId) -> io::Result<Option<Status>> {
        let key = checked_key(operation)?;
        if let Some(record) = self.records.get(&key) {
            same_operation(operation, &record.operation)?;
            if record.terminal.is_none() && self.poisoned.is_none() {
                self.reconcile(&key);
            }
            return Ok(Some(self.record_status(&key)));
        }
        if let Some(receipt) = self.temporary_outcomes.get(&key) {
            same_operation(operation, &receipt.operation)?;
            return Ok(Some(Status::Terminal(Box::new(receipt.clone()))));
        }
        if let Some(live) = self.receiving.get(&key) {
            same_operation(operation, &live.operation)?;
            return Ok(Some(Status::Receiving {
                ticket: live.ticket.clone(),
                received: live.stage.metadata()?.len(),
            }));
        }
        Ok(None)
    }

    pub fn begin(
        &mut self,
        operation: OperationId,
        attempt: Attempt,
        intent: Intent,
    ) -> io::Result<Status> {
        let key = checked_key(&operation)?;
        validate_attempt(&attempt)?;
        validate_intent(&intent)?;
        if let Some(record) = self.records.get(&key) {
            same_operation(&operation, &record.operation)?;
            same_intent(&intent, &record.intent)?;
            return Ok(self.lookup(&operation)?.expect("record exists"));
        }
        if let Some(receipt) = self.temporary_outcomes.get(&key) {
            same_operation(&operation, &receipt.operation)?;
            same_intent(&intent, &receipt.requested)?;
            return Ok(Status::Terminal(Box::new(receipt.clone())));
        }
        self.healthy()?;
        if let Some(live) = self.receiving.get(&key) {
            same_operation(&operation, &live.operation)?;
            same_intent(&intent, &live.intent)?;
            if attempt != live.attempt {
                return Err(invalid("live attempt must be explicitly replaced"));
            }
            return Ok(Status::Receiving {
                ticket: live.ticket.clone(),
                received: live.stage.metadata()?.len(),
            });
        }
        let total: u64 = self.receiving.values().map(|r| r.intent.bytes).sum();
        if self.receiving.len() >= MAX_TRANSFERS || total + intent.bytes > MAX_RECEIVING_BYTES {
            return Err(invalid("screenshot receive admission limit reached"));
        }
        // Bound process-local tombstones and undrained observations as well.
        if self.temporary_outcomes.len() >= 1024 || self.observations.len() >= 1024 {
            return Err(invalid("screenshot session capacity reached"));
        }
        let (destination, parent) = self.allowed_parent(&operation.slug, &intent.destination)?;
        let stage_name = format!(".wicket-screenshot-{key}-{}.stage", random_suffix()?);
        let stage = open_at(
            &parent,
            stage_name.as_ref(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
        )?;
        let ticket = self.ticket(&key);
        self.receiving.insert(
            key,
            Receiving {
                operation,
                attempt,
                intent,
                destination,
                parent,
                stage,
                stage_name,
                ticket: ticket.clone(),
            },
        );
        Ok(Status::Receiving {
            ticket,
            received: 0,
        })
    }

    pub fn append(&mut self, ticket: &Ticket, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.healthy()?;
        let live = self.live(ticket)?;
        if bytes.is_empty() || bytes.len() > MAX_CHUNK_BYTES {
            return Err(invalid("invalid screenshot chunk size"));
        }
        verify_live_stage(live)?;
        let length = live.stage.metadata()?.len();
        if length != offset || length.saturating_add(bytes.len() as u64) > live.intent.bytes {
            return Err(invalid("chunk offset or byte count exceeds manifest"));
        }
        let mut stage = &live.stage;
        stage.seek(SeekFrom::End(0))?;
        stage.write_all(bytes)
    }

    /// Explicit replacement fences the old ticket; it cannot truncate an
    /// accepted operation. Caller and immutable intent are retained.
    pub fn replace(&mut self, ticket: &Ticket, attempt: Attempt) -> io::Result<Ticket> {
        self.healthy()?;
        validate_attempt(&attempt)?;
        let live = self.live(ticket)?;
        if live.attempt.attempt_id == attempt.attempt_id {
            return Err(invalid("replacement needs a new attempt identity"));
        }
        verify_live_stage(live)?;
        live.stage.set_len(0)?;
        let new_ticket = self.ticket(&ticket.key);
        let live = self
            .receiving
            .get_mut(&ticket.key)
            .expect("live ticket checked");
        live.attempt = attempt;
        live.ticket = new_ticket.clone();
        Ok(new_ticket)
    }

    /// Cancel/disconnect must be sent to the same serialized owner as finish.
    /// Before acceptance it is teardown. Afterwards status alone may establish
    /// an outcome; cancellation cannot invent Aborted for uncertain publication.
    pub fn cancel(&mut self, operation: &OperationId, ticket: &Ticket) -> io::Result<Status> {
        let key = checked_key(operation)?;
        if key != ticket.key || ticket.owner != self.owner {
            return Err(invalid("stale or foreign ticket"));
        }
        if self.records.contains_key(&key) || self.temporary_outcomes.contains_key(&key) {
            return self
                .lookup(operation)?
                .ok_or_else(|| invalid("unknown operation"));
        }
        same_operation(operation, &self.live(ticket)?.operation)?;
        let live = self.receiving.remove(&key).expect("live ticket checked");
        let receipt = temporary_receipt(&live, Outcome::Aborted);
        self.cleanup_live(&live);
        self.observations.push(receipt_record(&receipt));
        self.temporary_outcomes.insert(key, receipt.clone());
        Ok(Status::Terminal(Box::new(receipt)))
    }

    pub fn finish(&mut self, operation: &OperationId, ticket: &Ticket) -> io::Result<Status> {
        let key = checked_key(operation)?;
        if key != ticket.key || ticket.owner != self.owner {
            return Err(invalid("stale or foreign ticket"));
        }
        if self.records.contains_key(&key) || self.temporary_outcomes.contains_key(&key) {
            return self
                .lookup(operation)?
                .ok_or_else(|| invalid("unknown operation"));
        }
        self.healthy()?;
        same_operation(operation, &self.live(ticket)?.operation)?;
        if let Err((action, error)) = self.accept(ticket) {
            // A failed journal barrier can still have landed a record. Keep its
            // stage and freeze the capability; never turn that into Failed.
            if self.records.contains_key(&key) {
                return Ok(self.record_status(&key));
            }
            let live = self
                .receiving
                .remove(&key)
                .expect("pre-acceptance transfer retained");
            let receipt = temporary_receipt(
                &live,
                Outcome::Failed {
                    action: action.into(),
                    reason: error.to_string(),
                },
            );
            self.cleanup_live(&live);
            self.observations.push(receipt_record(&receipt));
            self.temporary_outcomes.insert(key, receipt.clone());
            return Ok(Status::Terminal(Box::new(receipt)));
        }
        self.publish_accepted(&key);
        Ok(self.record_status(&key))
    }

    fn accept(&mut self, ticket: &Ticket) -> Result<(), (&'static str, io::Error)> {
        let live = self.live(ticket).map_err(|e| ("prepare", e))?;
        let verified = (|| {
            verify_live_stage(live)?;
            let mut stage = &live.stage;
            stage.seek(SeekFrom::Start(0))?;
            let bytes = read_bounded(live.stage.try_clone()?, MAX_PNG_BYTES)?;
            verify_bytes(&live.intent, &bytes)?;
            let (pixel_width, pixel_height) = validate_png(&bytes)?;
            Ok(VerifiedPng {
                mime_type: "image/png".into(),
                bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                pixel_width,
                pixel_height,
            })
        })()
        .map_err(|e| ("verify", e))?;
        let (destination, parent, parent_identity, stage_identity) = (|| {
            let (destination, parent) =
                self.allowed_parent(&live.operation.slug, &live.intent.destination)?;
            let parent_identity = Identity::of(&parent)?;
            if destination != live.destination || parent_identity != Identity::of(&live.parent)? {
                return Err(invalid("destination parent moved or was replaced"));
            }
            verify_live_stage(live)?;
            Ok((
                destination,
                parent,
                parent_identity,
                Identity::of(&live.stage)?,
            ))
        })()
        .map_err(|e| ("prepare", e))?;
        live.stage.sync_all().map_err(|e| ("sync", e))?;
        parent.sync_all().map_err(|e| ("sync", e))?;
        let record = Record {
            version: 2,
            operation: live.operation.clone(),
            attempt: live.attempt.clone(),
            intent: live.intent.clone(),
            destination,
            parent_identity,
            stage_name: live.stage_name.clone(),
            stage_identity,
            verified,
            accepted_at: chrono::Utc::now().to_rfc3339(),
            terminal: None,
        };
        let key = ticket.key.clone();
        // From this point onward preserve the stage even if persistence fails.
        self.receiving.remove(&key);
        self.records.insert(key.clone(), record.clone());
        self.persist(&key, &record).map_err(|e| ("persist", e))
    }

    fn publish_accepted(&mut self, key: &str) {
        let record = self.records[key].clone();
        let result = (|| {
            let parent = self.record_parent(&record)?;
            let _stage = record_stage(&record, &parent)?;
            link_at(
                &parent,
                &record.stage_name,
                record.destination.file_name().expect("validated filename"),
            )?;
            self.known_publication.insert(key.to_owned(), true);
            #[cfg(test)]
            self.check_fault(Fault::AfterLink)?;
            let sync = self.sync_published_parent(&parent);
            let outcome = match sync {
                Ok(()) => Outcome::Stored,
                Err(error) => Outcome::PublishedDurabilityUnconfirmed {
                    reason: error.to_string(),
                },
            };
            self.terminalize(key, outcome)
        })();
        if let Err(error) = result {
            if self.known_publication.get(key) == Some(&true) || self.poisoned.is_some() {
                return;
            }
            // These are definitive local no-clobber refusals. Other errors keep
            // Accepted evidence, rather than guessing about filesystem effects.
            if error.kind() == io::ErrorKind::AlreadyExists {
                self.known_publication.insert(key.to_owned(), false);
                let _ = self.terminalize(
                    key,
                    Outcome::Failed {
                        action: "publish".into(),
                        reason: "destination already exists; nothing replaced".into(),
                    },
                );
            }
        }
    }

    fn reconcile(&mut self, key: &str) {
        if self.poisoned.is_some() || self.records[key].terminal.is_some() {
            return;
        }
        let record = self.records[key].clone();
        // Recovery establishes only positive publication evidence. It does not
        // recreate a missing destination or guess what a replaced one means.
        let proof = (|| {
            let parent = self.record_parent(&record)?;
            let stage = record_stage(&record, &parent)?;
            let destination = open_at(
                &parent,
                record.destination.file_name().expect("validated filename"),
                libc::O_RDONLY,
            )?;
            if Identity::of(&stage)? != Identity::of(&destination)? {
                return Err(invalid("publication evidence differs"));
            }
            let bytes = read_bounded(stage.try_clone()?, MAX_PNG_BYTES)?;
            verify_bytes(&record.intent, &bytes)?;
            stage.sync_all()?;
            Ok(parent)
        })();
        if let Ok(parent) = proof {
            self.known_publication.insert(key.to_owned(), true);
            let outcome = match self.sync_published_parent(&parent) {
                Ok(()) => Outcome::Stored,
                Err(error) => Outcome::PublishedDurabilityUnconfirmed {
                    reason: error.to_string(),
                },
            };
            let _ = self.terminalize(key, outcome);
        }
    }

    fn terminalize(&mut self, key: &str, outcome: Outcome) -> io::Result<()> {
        let mut record = self.records[key].clone();
        if record.terminal.is_some() {
            return Err(invalid("terminal receipt is immutable"));
        }
        let receipt = Receipt {
            operation: record.operation.clone(),
            attempt: record.attempt.clone(),
            requested: record.intent.clone(),
            event_id: format!("screenshot-{key}-terminal"),
            destination: record.destination.clone(),
            verified: Some(record.verified.clone()),
            accepted_at: Some(record.accepted_at.clone()),
            observed_at: chrono::Utc::now().to_rfc3339(),
            outcome,
        };
        record.terminal = Some(receipt.clone());
        self.persist(key, &record)?;
        self.records.insert(key.to_owned(), record.clone());
        self.observations.push(receipt_record(&receipt));
        self.cleanup_record(&record);
        Ok(())
    }

    fn persist(&mut self, key: &str, record: &Record) -> io::Result<()> {
        self.healthy()?;
        let result = (|| {
            let bytes = serde_json::to_vec(record).map_err(invalid)?;
            if bytes.len() as u64 > MAX_JOURNAL_BYTES {
                return Err(invalid("journal entry exceeds limit"));
            }
            let pending = format!("{key}.new");
            let mut file = open_at(
                &self.directory,
                pending.as_ref(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            )?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            #[cfg(test)]
            self.check_fault(Fault::BeforeJournalRename)?;
            rename_at(&self.directory, &pending, &format!("{key}.json"))?;
            #[cfg(test)]
            self.check_fault(Fault::AfterJournalRename)?;
            self.directory.sync_all()
        })();
        if let Err(error) = &result {
            self.poisoned = Some(format!(
                "journal synchronization uncertain: {error}; reopen the capability"
            ));
        }
        result
    }

    fn record_status(&self, key: &str) -> Status {
        let record = &self.records[key];
        if let Some(receipt) = &record.terminal {
            return Status::Terminal(Box::new(receipt.clone()));
        }
        Status::Unresolved {
            operation: record.operation.clone(),
            destination: record.destination.clone(),
            published: self.known_publication.get(key).copied(),
            reason: self.poisoned.clone().unwrap_or_else(|| {
                "publication evidence is incomplete; no automatic continuation".into()
            }),
        }
    }

    fn ticket(&mut self, key: &str) -> Ticket {
        self.next_generation += 1;
        Ticket {
            key: key.to_owned(),
            owner: self.owner,
            generation: self.next_generation,
        }
    }
    fn live(&self, ticket: &Ticket) -> io::Result<&Receiving> {
        let live = self
            .receiving
            .get(&ticket.key)
            .ok_or_else(|| invalid("no live receive for ticket"))?;
        if &live.ticket != ticket {
            return Err(invalid("stale or foreign ticket"));
        }
        Ok(live)
    }
    fn healthy(&self) -> io::Result<()> {
        match &self.poisoned {
            Some(reason) => Err(io::Error::other(reason.clone())),
            None => Ok(()),
        }
    }
    fn sync_published_parent(&mut self, parent: &File) -> io::Result<()> {
        #[cfg(test)]
        self.check_fault(Fault::ParentSync)?;
        parent.sync_all()
    }
    fn cleanup_live(&mut self, live: &Receiving) {
        if verify_live_stage(live).is_ok() {
            self.cleanup_name(
                &live.parent,
                &live.stage_name,
                &live.destination,
                &live.operation,
            );
        }
    }
    fn cleanup_record(&mut self, record: &Record) {
        if let Ok(parent) = self.record_parent(record) {
            if record_stage(record, &parent).is_ok() {
                self.cleanup_name(
                    &parent,
                    &record.stage_name,
                    &record.destination,
                    &record.operation,
                );
            }
        }
    }
    fn cleanup_name(
        &mut self,
        parent: &File,
        name: &str,
        destination: &Path,
        operation: &OperationId,
    ) {
        let mut action = "unlink";
        let result = (|| {
            #[cfg(test)]
            self.check_fault(Fault::Cleanup)?;
            unlink_at(parent, name.as_ref())?;
            action = "sync";
            #[cfg(test)]
            self.check_fault(Fault::CleanupSync)?;
            parent.sync_all()
        })();
        if let Err(error) = result {
            self.observations.push(json!({ "who": "screenshot", "what": format!("fail_{action}"),
                "where": destination.parent().expect("validated parent").join(name), "why": error.to_string(),
                "how": "unlinkat and directory synchronization", "with": { "operation": operation, "unlinked": action == "sync" } }));
        }
    }
    fn record_parent(&self, record: &Record) -> io::Result<File> {
        let (path, parent) =
            self.allowed_parent(&record.operation.slug, &record.intent.destination)?;
        if path != record.destination || Identity::of(&parent)? != record.parent_identity {
            return Err(invalid("destination parent moved or was replaced"));
        }
        Ok(parent)
    }
    fn allowed_parent(&self, slug: &str, destination: &Path) -> io::Result<(PathBuf, File)> {
        validate_id(slug)?;
        validate_absolute(destination)?;
        let mut roots = vec![self.home.join("pane").join(slug)];
        let config = self
            .home
            .join(".local/state/wicket")
            .join(slug)
            .join("sandbox.conf");
        match fs::read_to_string(config) {
            Ok(config) => {
                for line in config.lines().map(str::trim) {
                    if let Some(root) = line.strip_prefix("writable ") {
                        let root = root.trim();
                        roots.push(match root.strip_prefix("~/") {
                            Some(relative) => self.home.join(relative),
                            None => PathBuf::from(root),
                        });
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let parent_path = fs::canonicalize(
            destination
                .parent()
                .ok_or_else(|| invalid("missing parent"))?,
        )?;
        let mut allowed = false;
        for root in roots {
            if !root.is_absolute() {
                return Err(invalid("configured writable root is not absolute"));
            }
            match fs::canonicalize(root) {
                Ok(root) if parent_path.starts_with(&root) => {
                    allowed = true;
                    break;
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        if !allowed {
            return Err(invalid("destination is outside the slug's writable roots"));
        }
        let destination = parent_path.join(destination.file_name().expect("validated filename"));
        if destination.as_os_str().len() > 4096
            || serde_json::to_vec(&destination).map_err(invalid)?.len()
                > MAX_CANONICAL_PATH_JSON_BYTES
        {
            return Err(invalid("canonical destination exceeds metadata limit"));
        }
        let parent = open_directory(&parent_path)?;
        Ok((destination, parent))
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Graceful shutdown discards only unaccepted receives. A process crash
        // may leave uniquely named .stage litter; it is never adopted or swept.
        let live: Vec<_> = self.receiving.drain().map(|(_, live)| live).collect();
        for receive in live {
            self.cleanup_live(&receive);
        }
    }
}

fn checked_key(operation: &OperationId) -> io::Result<String> {
    validate_id(&operation.caller)?;
    operation_key(&operation.slug, &operation.operation_id)
}
fn validate_attempt(attempt: &Attempt) -> io::Result<()> {
    validate_id(&attempt.call_id)?;
    validate_id(&attempt.attempt_id)
}
fn same_operation(a: &OperationId, b: &OperationId) -> io::Result<()> {
    if a != b {
        return Err(invalid("operation identity or caller conflicts"));
    }
    Ok(())
}
fn same_intent(a: &Intent, b: &Intent) -> io::Result<()> {
    if a != b {
        return Err(invalid("operation ID already binds different intent"));
    }
    Ok(())
}
fn random_suffix() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn verify_live_stage(live: &Receiving) -> io::Result<()> {
    let named = open_at(&live.parent, live.stage_name.as_ref(), libc::O_RDONLY)?;
    if !named.metadata()?.is_file()
        || Identity::of(&named)? != Identity::of(&live.stage)?
        || live.stage.metadata()?.nlink() != 1
    {
        return Err(invalid("receiving stage was replaced or hard-linked"));
    }
    Ok(())
}
fn record_stage(record: &Record, parent: &File) -> io::Result<File> {
    let stage = open_at(parent, record.stage_name.as_ref(), libc::O_RDONLY)?;
    if !stage.metadata()?.is_file() || Identity::of(&stage)? != record.stage_identity {
        return Err(invalid("staging evidence was replaced"));
    }
    Ok(stage)
}
fn temporary_receipt(live: &Receiving, outcome: Outcome) -> Receipt {
    Receipt {
        operation: live.operation.clone(),
        attempt: live.attempt.clone(),
        requested: live.intent.clone(),
        event_id: format!("{}-{}-temporary", live.stage_name, live.ticket.generation),
        destination: live.destination.clone(),
        verified: None,
        accepted_at: None,
        observed_at: chrono::Utc::now().to_rfc3339(),
        outcome,
    }
}
fn receipt_record(receipt: &Receipt) -> Value {
    let (what, why) = match &receipt.outcome {
        Outcome::Stored => ("store".to_owned(), None),
        Outcome::PublishedDurabilityUnconfirmed { reason } => {
            ("fail_sync".to_owned(), Some(reason))
        }
        Outcome::Failed { action, reason } => (format!("fail_{action}"), Some(reason)),
        Outcome::Aborted => ("abort".to_owned(), None),
    };
    let how = match &receipt.outcome {
        Outcome::Aborted => "receiving ticket invalidation",
        Outcome::PublishedDurabilityUnconfirmed { .. } => {
            "directory synchronization after atomic no-clobber linkat"
        }
        Outcome::Failed { action, .. } if action == "verify" => {
            "PNG decode, byte count and SHA-256 verification"
        }
        Outcome::Failed { action, .. } if action == "prepare" => "rooted staging-file preparation",
        Outcome::Failed { action, .. } if action == "sync" => {
            "staging-file and parent synchronization"
        }
        _ => "rooted filesystem / atomic no-clobber linkat",
    };
    let mut value = json!({ "who": "screenshot", "what": what, "where": receipt.destination,
        "how": how, "with": { "receipt": receipt } });
    if let Some(reason) = why {
        value["why"] = json!(reason);
    }
    if matches!(
        receipt.outcome,
        Outcome::PublishedDurabilityUnconfirmed { .. }
    ) {
        value["with"]["durability"] = json!("unconfirmed");
        value["with"]["published"] = json!(true);
    }
    value
}
fn validate_record(key: &str, record: &Record) -> io::Result<()> {
    validate_intent(&record.intent)?;
    validate_absolute(&record.destination)?;
    validate_attempt(&record.attempt)?;
    let prefix = format!(".wicket-screenshot-{key}-");
    let suffix = record
        .stage_name
        .strip_prefix(&prefix)
        .and_then(|s| s.strip_suffix(".stage"))
        .ok_or_else(|| invalid("invalid stage name"))?;
    if record.version != 2
        || checked_key(&record.operation)? != key
        || suffix.len() != 32
        || !suffix.bytes().all(|c| c.is_ascii_hexdigit())
    {
        return Err(invalid("invalid journal version or identity"));
    }
    let verified = &record.verified;
    if verified.mime_type != "image/png"
        || verified.bytes != record.intent.bytes
        || verified.sha256 != record.intent.sha256
        || verified.pixel_width == 0
        || verified.pixel_height == 0
        || verified.pixel_width > MAX_DIMENSION
        || verified.pixel_height > MAX_DIMENSION
        || u64::from(verified.pixel_width) * u64::from(verified.pixel_height) > MAX_PIXELS
    {
        return Err(invalid("invalid verified PNG facts"));
    }
    if let Some(receipt) = &record.terminal {
        if receipt.operation != record.operation
            || receipt.attempt != record.attempt
            || receipt.requested != record.intent
            || receipt.destination != record.destination
            || receipt.verified.as_ref() != Some(verified)
            || receipt.accepted_at.as_ref() != Some(&record.accepted_at)
            || receipt.event_id != format!("screenshot-{key}-terminal")
            || matches!(receipt.outcome, Outcome::Aborted)
        {
            return Err(invalid(
                "terminal receipt does not match accepted operation",
            ));
        }
    }
    Ok(())
}
fn link_at(parent: &File, from: &str, to: &OsStr) -> io::Result<()> {
    let from = c_name(from.as_ref())?;
    let to = c_name(to)?;
    // SAFETY: both single-component names and the directory remain live.
    if unsafe {
        libc::linkat(
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn validate_id(id: &str) -> io::Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(invalid(
            "identity must contain 1..128 ASCII letters, digits, hyphens or underscores",
        ));
    }
    Ok(())
}

fn operation_key(slug: &str, operation_id: &str) -> io::Result<String> {
    validate_id(slug)?;
    validate_id(operation_id)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(format!("{slug}\0{operation_id}"))
    ))
}

fn validate_key(key: &str) -> io::Result<()> {
    if key.len() != 64
        || !key
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(invalid("invalid journal key or SHA-256"));
    }
    Ok(())
}

fn validate_absolute(path: &Path) -> io::Result<()> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.as_os_str().as_bytes().ends_with(b"/")
        || path
            .as_os_str()
            .as_bytes()
            .split(|c| *c == b'/')
            .any(|c| c == b"." || c == b"..")
    {
        return Err(invalid(
            "destination must be absolute without dot or parent components",
        ));
    }
    Ok(())
}

fn validate_intent(intent: &Intent) -> io::Result<()> {
    validate_absolute(&intent.destination)?;
    validate_key(&intent.sha256)?;
    if intent.bytes == 0 || intent.bytes > MAX_PNG_BYTES {
        return Err(invalid("PNG byte count exceeds limit or is empty"));
    }
    if intent.source_url.len() > 8192
        || intent.captured_at.len() > 128
        || intent.destination.as_os_str().len() > 4096
    {
        return Err(invalid("capture metadata exceeds limit"));
    }
    if serde_json::to_vec(intent).map_err(invalid)?.len() > MAX_INTENT_JSON_BYTES {
        return Err(invalid("serialized capture metadata exceeds limit"));
    }
    Ok(())
}

fn verify_bytes(intent: &Intent, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() as u64 != intent.bytes || format!("{:x}", Sha256::digest(bytes)) != intent.sha256
    {
        return Err(invalid(
            "screenshot byte count or SHA-256 does not match manifest",
        ));
    }
    Ok(())
}

fn validate_png(bytes: &[u8]) -> io::Result<(u32, u32)> {
    // Check framing through IEND explicitly: a decoder may otherwise accept a
    // valid image followed by garbage. PNG's decoder checks CRCs and pixels.
    if !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Err(invalid("not a PNG"));
    }
    let mut offset = 8usize;
    loop {
        let header = bytes
            .get(offset..offset + 8)
            .ok_or_else(|| invalid("truncated PNG chunk"))?;
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let end = offset
            .checked_add(12)
            .and_then(|n| n.checked_add(length))
            .ok_or_else(|| invalid("PNG chunk overflow"))?;
        if end > bytes.len() || &header[4..8] == b"acTL" {
            return Err(invalid("truncated or animated PNG is not a screenshot"));
        }
        if &header[4..8] == b"IEND" {
            if length != 0 || end != bytes.len() {
                return Err(invalid("invalid PNG end or trailing bytes"));
            }
            break;
        }
        offset = end;
    }
    let mut options = png::DecodeOptions::default();
    options.set_skip_ancillary_crc_failures(false);
    let mut decoder = png::Decoder::new_with_options(Cursor::new(bytes), options);
    decoder.set_limits(png::Limits {
        bytes: MAX_DECODE_BYTES,
    });
    decoder.set_ignore_text_chunk(true);
    decoder.set_ignore_iccp_chunk(true);
    let info = decoder.read_header_info().map_err(invalid)?;
    let (width, height) = (info.width, info.height);
    if width == 0
        || height == 0
        || width > MAX_DIMENSION
        || height > MAX_DIMENSION
        || u64::from(width) * u64::from(height) > MAX_PIXELS
    {
        return Err(invalid("decoded PNG dimensions exceed limit"));
    }
    let mut reader = decoder.read_info().map_err(invalid)?;
    let size = reader
        .output_buffer_size()
        .filter(|size| *size <= MAX_DECODE_BYTES)
        .ok_or_else(|| invalid("decoded PNG buffer exceeds limit"))?;
    let mut pixels = vec![0; size];
    reader.next_frame(&mut pixels).map_err(invalid)?;
    reader.finish().map_err(invalid)?;
    Ok((width, height))
}

fn read_bounded(file: File, limit: u64) -> io::Result<Vec<u8>> {
    if !file.metadata()?.is_file() || file.metadata()?.len() > limit {
        return Err(invalid("file is not regular or exceeds limit"));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid("file exceeds limit"));
    }
    Ok(bytes)
}

fn open_directory(path: &Path) -> io::Result<File> {
    if !path.is_absolute() {
        return Err(invalid("directory is not absolute"));
    }
    let mut directory = File::open("/")?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = open_at(&directory, name, libc::O_RDONLY | libc::O_DIRECTORY)?
            }
            _ => return Err(invalid("directory contains dot or parent components")),
        }
    }
    Ok(directory)
}

fn c_name(name: &OsStr) -> io::Result<CString> {
    if name.as_bytes().contains(&b'/') || name == "." || name == ".." {
        return Err(invalid("expected one path component"));
    }
    CString::new(name.as_bytes()).map_err(invalid)
}

fn open_at(parent: &File, name: &OsStr, flags: i32) -> io::Result<File> {
    let name = c_name(name)?;
    // SAFETY: openat consumes the string during this call; successful fd ownership
    // is transferred exactly once. NOFOLLOW applies to every walked component.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn mkdir_at(parent: &File, name: &str) -> io::Result<()> {
    let name = c_name(name.as_ref())?;
    // SAFETY: parent and name remain live for the call.
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
    }
    parent.sync_all()
}

fn unlink_at(parent: &File, name: &OsStr) -> io::Result<()> {
    let name = c_name(name)?;
    // SAFETY: parent and name remain live for the call.
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn rename_at(parent: &File, from: &str, to: &str) -> io::Result<()> {
    let from = c_name(from.as_ref())?;
    let to = c_name(to.as_ref())?;
    // SAFETY: both names and the directory remain live for the call.
    if unsafe {
        libc::renameat(
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn invalid(message: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    AfterLink,
    BeforeJournalRename,
    AfterJournalRename,
    ParentSync,
    Cleanup,
    CleanupSync,
}

#[cfg(test)]
impl Store {
    fn check_fault(&mut self, point: Fault) -> io::Result<()> {
        if self.fault == Some(point) {
            self.fault = None;
            return Err(io::Error::other(format!("injected {point:?}")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
