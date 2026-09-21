use std::os::unix::fs::symlink;
use std::process::Command;

use super::*;

struct Fixture {
    home: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let home = std::env::temp_dir().join(format!(
            "wicket-screenshot-test-{}",
            random_suffix().unwrap()
        ));
        fs::create_dir_all(home.join("pane/test")).unwrap();
        Self {
            home: fs::canonicalize(home).unwrap(),
        }
    }
    fn store(&self) -> Store {
        Store::open(&self.home).unwrap()
    }
    fn destination(&self) -> PathBuf {
        self.home.join("pane/test/capture.png")
    }
    fn journal(&self) -> PathBuf {
        self.home.join(".local/state/wicket/screenshots")
    }
    fn records(&self) -> Vec<PathBuf> {
        fs::read_dir(self.journal())
            .unwrap()
            .map(|p| p.unwrap().path())
            .filter(|p| p.extension().is_some_and(|s| s == "json"))
            .collect()
    }
    fn stages(&self) -> Vec<PathBuf> {
        fs::read_dir(self.destination().parent().unwrap())
            .unwrap()
            .map(|p| p.unwrap().path())
            .filter(|p| p.extension().is_some_and(|s| s == "stage"))
            .collect()
    }
    fn configure(&self, root: &str) {
        let directory = self.home.join(".local/state/wicket/test");
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("sandbox.conf"), format!("writable {root}\n")).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.home);
    }
}

fn operation() -> OperationId {
    OperationId {
        slug: "test".into(),
        caller: "shotgun".into(),
        operation_id: "save-1".into(),
    }
}

fn attempt(id: &str) -> Attempt {
    Attempt {
        call_id: format!("call-{id}"),
        attempt_id: id.into(),
    }
}

fn png_bytes() -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 2, 1);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_image_data(&[255, 0, 0, 255, 0, 128, 255, 255])
            .unwrap();
        writer.finish().unwrap();
    }
    bytes
}

fn intent(path: PathBuf, bytes: &[u8]) -> Intent {
    Intent {
        destination: path,
        bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        mode: CaptureMode::Viewport,
        source_url: "https://example.test/capture".into(),
        captured_at: "2026-09-20T20:00:00Z".into(),
    }
}

fn ticket(status: Status) -> Ticket {
    match status {
        Status::Receiving { ticket, .. } => ticket,
        other => panic!("expected receiving: {other:?}"),
    }
}

fn receipt(status: Status) -> Receipt {
    match status {
        Status::Terminal(receipt) => *receipt,
        other => panic!("expected terminal: {other:?}"),
    }
}

fn upload(store: &mut Store, intent: Intent, bytes: &[u8]) -> Ticket {
    let ticket = ticket(store.begin(operation(), attempt("a"), intent).unwrap());
    for (index, chunk) in bytes.chunks(MAX_CHUNK_BYTES).enumerate() {
        store
            .append(&ticket, (index * MAX_CHUNK_BYTES) as u64, chunk)
            .unwrap();
    }
    ticket
}

fn assert_unresolved(status: Status) {
    assert!(matches!(status, Status::Unresolved { .. }), "{status:?}");
}

#[test]
fn exact_publication_has_verified_facts_and_one_correlated_observation() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let intent = intent(fixture.destination(), &bytes);
    let ticket = upload(&mut store, intent.clone(), &bytes);
    assert!(fixture.records().is_empty());
    assert!(store.take_observations().is_empty());
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    assert_eq!(saved.outcome, Outcome::Stored);
    assert_eq!(saved.requested, intent);
    assert_eq!(saved.operation, operation());
    assert_eq!(saved.attempt, attempt("a"));
    assert!(saved.accepted_at.is_some());
    assert_eq!(
        saved.verified,
        Some(VerifiedPng {
            mime_type: "image/png".into(),
            bytes: bytes.len() as u64,
            sha256: intent.sha256,
            pixel_width: 2,
            pixel_height: 1
        })
    );
    assert_eq!(fs::read(fixture.destination()).unwrap(), bytes);
    assert_eq!(
        fs::metadata(fixture.destination())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(fixture.stages().is_empty());
    let observations = store.take_observations();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(observation["what"], "store");
    assert_eq!(observation["who"], "screenshot");
    assert!(observation.get("whom").is_none());
    assert_eq!(observation["with"]["receipt"]["event_id"], saved.event_id);
    assert!(observation["how"].as_str().unwrap().contains("linkat"));
    assert_eq!(receipt(store.lookup(&operation()).unwrap().unwrap()), saved);
    assert_eq!(receipt(store.finish(&operation(), &ticket).unwrap()), saved);
    assert_eq!(receipt(store.cancel(&operation(), &ticket).unwrap()), saved);
    assert!(store.take_observations().is_empty());
}

#[test]
fn completed_retry_and_lookup_are_historical_and_immutable_across_reopen() {
    let fixture = Fixture::new();
    let bytes = png_bytes();
    let intent = intent(fixture.destination(), &bytes);
    let mut store = fixture.store();
    let ticket = upload(&mut store, intent.clone(), &bytes);
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    let journal = fs::read(&fixture.records()[0]).unwrap();
    drop(store);
    fs::remove_file(fixture.destination()).unwrap();
    fs::write(fixture.destination(), b"later occupant").unwrap();
    let mut store = fixture.store();
    assert_eq!(receipt(store.lookup(&operation()).unwrap().unwrap()), saved);
    assert_eq!(
        receipt(store.begin(operation(), attempt("retry"), intent).unwrap()),
        saved
    );
    assert_eq!(fs::read(&fixture.records()[0]).unwrap(), journal);
    assert_eq!(fs::read(fixture.destination()).unwrap(), b"later occupant");
    assert!(store.take_observations().is_empty());
    assert!(store.finish(&operation(), &ticket).is_err());
}

#[test]
fn receiving_is_ephemeral_and_graceful_drop_removes_only_its_stage() {
    let fixture = Fixture::new();
    let bytes = png_bytes();
    let mut store = fixture.store();
    let ticket = upload(
        &mut store,
        intent(fixture.destination(), &bytes),
        &bytes[..20],
    );
    assert_eq!(fixture.stages().len(), 1);
    assert!(fixture.records().is_empty());
    drop(store);
    assert!(fixture.stages().is_empty());
    let mut store = fixture.store();
    assert_eq!(store.lookup(&operation()).unwrap(), None);
    assert!(store.append(&ticket, 20, &bytes[20..]).is_err());
}

#[test]
fn no_clobber_includes_files_directories_and_symlinks() {
    for occupant in ["file", "directory", "symlink"] {
        let fixture = Fixture::new();
        let bytes = png_bytes();
        let original = fixture.home.join("original");
        fs::write(&original, b"original").unwrap();
        match occupant {
            "file" => fs::write(fixture.destination(), b"original").unwrap(),
            "directory" => fs::create_dir(fixture.destination()).unwrap(),
            _ => symlink(&original, fixture.destination()).unwrap(),
        }
        let before = fs::symlink_metadata(fixture.destination()).unwrap().ino();
        let mut store = fixture.store();
        let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
        let failed = receipt(store.finish(&operation(), &ticket).unwrap());
        assert!(
            matches!(failed.outcome, Outcome::Failed { ref action, .. } if action == "publish")
        );
        assert!(failed.accepted_at.is_some());
        assert_eq!(
            fs::symlink_metadata(fixture.destination()).unwrap().ino(),
            before
        );
        assert_eq!(fs::read(&original).unwrap(), b"original");
        assert!(fixture.stages().is_empty());
        drop(store);
        assert_eq!(
            receipt(fixture.store().lookup(&operation()).unwrap().unwrap()),
            failed
        );
    }
}

#[test]
fn absolute_existing_parent_and_component_containment_are_required() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    fs::create_dir(fixture.home.join("pane/test-sibling")).unwrap();
    let paths = [
        PathBuf::from("relative.png"),
        fixture.home.join("pane/test/../escape.png"),
        fixture.home.join("pane/test/./dot.png"),
        fixture.home.join("pane/test/missing/image.png"),
        fixture.home.join("pane/test-sibling/image.png"),
        fixture.home.join("outside.png"),
        PathBuf::from(format!("{}/", fixture.destination().display())),
    ];
    for path in paths {
        assert!(
            store
                .begin(operation(), attempt("a"), intent(path.clone(), &bytes))
                .is_err(),
            "{path:?}"
        );
    }
    assert!(fixture.records().is_empty());
    assert!(fixture.stages().is_empty());
    assert!(store.take_observations().is_empty());
}

#[test]
fn canonical_aliases_work_but_symlink_escape_is_refused() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let outside = fixture.home.join("outside");
    fs::create_dir(&outside).unwrap();
    symlink(&outside, fixture.home.join("pane/test/escape")).unwrap();
    assert!(store
        .begin(
            operation(),
            attempt("a"),
            intent(fixture.home.join("pane/test/escape/no.png"), &bytes)
        )
        .is_err());
    let alias = fixture.home.join("alias");
    symlink(fixture.home.join("pane/test"), &alias).unwrap();
    let requested = alias.join("capture.png");
    let ticket = upload(&mut store, intent(requested.clone(), &bytes), &bytes);
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    assert_eq!(saved.destination, fixture.destination());
    assert_eq!(saved.requested.destination, requested);
}

#[test]
fn configured_writable_roots_expand_home_and_remain_slug_scoped() {
    let fixture = Fixture::new();
    let bytes = png_bytes();
    let configured = fixture.home.join("exports");
    fs::create_dir(&configured).unwrap();
    fixture.configure("~/exports");
    let mut store = fixture.store();
    let ticket = upload(
        &mut store,
        intent(configured.join("capture.png"), &bytes),
        &bytes,
    );
    assert_eq!(
        receipt(store.finish(&operation(), &ticket).unwrap()).outcome,
        Outcome::Stored
    );
    let mut foreign = operation();
    foreign.slug = "other".into();
    assert!(store
        .begin(
            foreign,
            attempt("a"),
            intent(configured.join("foreign.png"), &bytes)
        )
        .is_err());
}

#[test]
fn macos_tmp_alias_is_canonicalized() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let fixture = Fixture::new();
    let alias = PathBuf::from("/tmp").join(format!("wicket-alias-{}", random_suffix().unwrap()));
    fs::create_dir(&alias).unwrap();
    fixture.configure(alias.to_str().unwrap());
    let mut store = fixture.store();
    let bytes = png_bytes();
    let path = alias.join("capture.png");
    let ticket = upload(&mut store, intent(path, &bytes), &bytes);
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    assert_eq!(
        saved.destination,
        fs::canonicalize(&alias).unwrap().join("capture.png")
    );
    fs::remove_dir_all(alias).unwrap();
}

#[test]
fn parent_replacement_and_policy_revocation_before_acceptance_prevent_publication() {
    for change in ["parent", "policy"] {
        let fixture = Fixture::new();
        let bytes = png_bytes();
        let root = fixture.home.join("exports");
        fs::create_dir(&root).unwrap();
        fixture.configure(root.to_str().unwrap());
        let mut store = fixture.store();
        let ticket = upload(&mut store, intent(root.join("capture.png"), &bytes), &bytes);
        if change == "parent" {
            fs::rename(&root, fixture.home.join("moved")).unwrap();
            symlink(fixture.home.join("pane/test"), &root).unwrap();
        } else {
            fixture.configure("/nonexistent-wicket-test-root");
        }
        let failed = receipt(store.finish(&operation(), &ticket).unwrap());
        assert!(matches!(failed.outcome, Outcome::Failed { .. }));
        assert!(failed.accepted_at.is_none());
        assert!(fixture.records().is_empty());
        assert!(!root.join("capture.png").exists());
        assert!(!fixture.home.join("moved/capture.png").exists());
    }
}

#[test]
fn substituted_stage_symlink_and_hardlink_cannot_receive_or_publish() {
    for substitution in ["symlink", "hardlink"] {
        let fixture = Fixture::new();
        let mut store = fixture.store();
        let bytes = png_bytes();
        let ticket = ticket(
            store
                .begin(
                    operation(),
                    attempt("a"),
                    intent(fixture.destination(), &bytes),
                )
                .unwrap(),
        );
        let stage = fixture.stages().pop().unwrap();
        let other = fixture.home.join("other");
        if substitution == "symlink" {
            fs::write(&other, b"unrelated").unwrap();
            fs::remove_file(&stage).unwrap();
            symlink(&other, &stage).unwrap();
        } else {
            fs::hard_link(&stage, &other).unwrap();
        }
        let before = fs::read(&other).unwrap();
        assert!(store.append(&ticket, 0, &bytes).is_err());
        assert!(matches!(
            receipt(store.finish(&operation(), &ticket).unwrap()).outcome,
            Outcome::Failed { .. }
        ));
        assert_eq!(fs::read(other).unwrap(), before);
        assert!(!fixture.destination().exists());
        assert!(fixture.records().is_empty());
        assert!(fs::symlink_metadata(stage).is_ok());
    }
}

// Independent CRC construction lets the dimension tests reach the header
// policy instead of merely failing a stale checksum.
fn chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut out = (data.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc = !0u32;
    for byte in &out[4..] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    out.extend_from_slice(&(!crc).to_be_bytes());
    out
}

#[test]
fn malformed_pngs_are_rejected_before_durable_acceptance() {
    let png = png_bytes();
    let mut trailing = png.clone();
    trailing.push(0);
    let mut crc = png.clone();
    *crc.last_mut().unwrap() ^= 1;
    let mut idat_crc = png.clone();
    let index = idat_crc.windows(4).position(|w| w == b"IDAT").unwrap();
    idat_crc[index + 4] ^= 1;
    let mut animated = png.clone();
    animated.splice(33..33, chunk(b"acTL", &[0, 0, 0, 1, 0, 0, 0, 0]));
    let mut bad_text = png.clone();
    let mut text = chunk(b"tEXt", b"key\0value");
    *text.last_mut().unwrap() ^= 1;
    bad_text.splice(33..33, text);
    let mut bad_pixels = png[..33].to_vec();
    bad_pixels.extend(chunk(b"IDAT", b"invalid compressed pixels"));
    bad_pixels.extend(chunk(b"IEND", &[]));
    let mut no_pixels = png[..33].to_vec();
    no_pixels.extend(chunk(b"IEND", &[]));
    for (case, bytes) in [
        ("signature", b"not PNG".to_vec()),
        ("truncated", png[..png.len() - 12].to_vec()),
        ("trailing", trailing),
        ("IEND CRC", crc),
        ("IDAT CRC", idat_crc),
        ("APNG", animated),
        ("text CRC", bad_text),
        ("pixels with correct chunk CRC", bad_pixels),
        ("missing pixel data", no_pixels),
    ] {
        let fixture = Fixture::new();
        let mut store = fixture.store();
        let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
        let failed = receipt(store.finish(&operation(), &ticket).unwrap());
        assert!(
            matches!(failed.outcome, Outcome::Failed { .. }),
            "{case}: {failed:?}"
        );
        assert!(failed.accepted_at.is_none());
        let observations = store.take_observations();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["what"], "fail_verify");
        assert!(fixture.records().is_empty());
        assert!(fixture.stages().is_empty());
        assert!(!fixture.destination().exists());
    }
}

#[test]
fn decoded_dimension_and_pixel_limits_precede_allocation() {
    for (width, height) in [(MAX_DIMENSION + 1, 1u32), (8192, 8192), (0, 1)] {
        let mut png = png_bytes();
        let mut header = png[16..29].to_vec();
        header[0..4].copy_from_slice(&width.to_be_bytes());
        header[4..8].copy_from_slice(&height.to_be_bytes());
        png.splice(8..33, chunk(b"IHDR", &header));
        let error = validate_png(&png).unwrap_err();
        if width != 0 {
            assert!(error.to_string().contains("dimensions exceed"), "{error}");
        }
    }
}

#[test]
fn manifest_count_hash_chunk_offset_and_admission_limits_are_enforced() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let original = intent(fixture.destination(), &bytes);
    for count in [0, MAX_PNG_BYTES + 1] {
        let mut oversized = original.clone();
        oversized.bytes = count;
        assert!(store.begin(operation(), attempt("a"), oversized).is_err());
    }
    let ticket = ticket(store.begin(operation(), attempt("a"), original).unwrap());
    assert!(store.append(&ticket, 0, &[]).is_err());
    assert!(store
        .append(&ticket, 0, &vec![0; MAX_CHUNK_BYTES + 1])
        .is_err());
    assert!(store.append(&ticket, 1, &bytes).is_err());
    assert!(store.append(&ticket, 0, &vec![0; bytes.len() + 1]).is_err());
    store.append(&ticket, 0, &bytes[..20]).unwrap();
    assert!(store.append(&ticket, 0, &bytes[..20]).is_err());
    let failed = receipt(store.finish(&operation(), &ticket).unwrap());
    assert!(matches!(failed.outcome, Outcome::Failed { .. }));
    assert!(fixture.records().is_empty());
    drop(store);
    let mut store = fixture.store();
    let mut mismatch = intent(fixture.destination(), &bytes);
    mismatch.sha256 = "0".repeat(64);
    let ticket = upload(&mut store, mismatch, &bytes);
    let failed = receipt(store.finish(&operation(), &ticket).unwrap());
    assert!(
        matches!(failed.outcome, Outcome::Failed { ref reason, .. } if reason.contains("SHA-256"))
    );
    assert!(!fixture.destination().exists());
    drop(store);
    let mut store = fixture.store();
    for number in 0..2 {
        let mut op = operation();
        op.operation_id = format!("large-{number}");
        let mut large = intent(fixture.destination(), &bytes);
        large.bytes = MAX_PNG_BYTES;
        store.begin(op, attempt("a"), large).unwrap();
    }
    assert!(store
        .begin(
            operation(),
            attempt("a"),
            intent(fixture.destination(), &bytes)
        )
        .is_err());
}

#[test]
fn transfer_count_is_bounded_independently_of_bytes() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    for index in 0..MAX_TRANSFERS {
        let mut op = operation();
        op.operation_id = format!("op-{index}");
        store
            .begin(op, attempt("a"), intent(fixture.destination(), &bytes))
            .unwrap();
    }
    assert!(store
        .begin(
            operation(),
            attempt("a"),
            intent(fixture.destination(), &bytes)
        )
        .is_err());
}

#[test]
fn escaped_metadata_is_bounded_before_creating_a_stage() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let mut oversized = intent(fixture.destination(), &bytes);
    oversized.source_url = "\0".repeat(8192);
    assert!(store.begin(operation(), attempt("a"), oversized).is_err());
    assert!(fixture.stages().is_empty());
    assert!(fixture.records().is_empty());
    let mut large = intent(fixture.destination(), &bytes);
    large.source_url = "x".repeat(8192);
    let ticket = upload(&mut store, large, &bytes);
    assert_eq!(
        receipt(store.finish(&operation(), &ticket).unwrap()).outcome,
        Outcome::Stored
    );
    drop(store);
    assert_eq!(
        receipt(fixture.store().lookup(&operation()).unwrap().unwrap()).outcome,
        Outcome::Stored
    );
}

#[test]
fn identity_conflicts_never_change_bound_intent_or_caller() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let original = intent(fixture.destination(), &bytes);
    let ticket = upload(&mut store, original.clone(), &bytes);
    let mut conflicts = Vec::new();
    let mut c = original.clone();
    c.destination.set_file_name("other.png");
    conflicts.push(c);
    let mut c = original.clone();
    c.bytes += 1;
    conflicts.push(c);
    let mut c = original.clone();
    c.sha256 = "0".repeat(64);
    conflicts.push(c);
    let mut c = original.clone();
    c.source_url.push_str("/different");
    conflicts.push(c);
    let mut c = original.clone();
    c.mode = CaptureMode::FullPage;
    conflicts.push(c);
    let mut c = original.clone();
    c.captured_at.push('x');
    conflicts.push(c);
    for c in &conflicts {
        assert!(store.begin(operation(), attempt("a"), c.clone()).is_err());
    }
    let mut caller = operation();
    caller.caller = "other-caller".into();
    assert!(store.lookup(&caller).is_err());
    assert!(store
        .begin(caller.clone(), attempt("a"), original.clone())
        .is_err());
    assert!(store.finish(&caller, &ticket).is_err());
    assert!(store.cancel(&caller, &ticket).is_err());
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    for c in conflicts {
        assert!(store.begin(operation(), attempt("a"), c).is_err());
    }
    assert_eq!(saved.requested, original);
}

#[test]
fn explicit_attempt_replacement_fences_stale_chunks_finish_and_cancel() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let original = intent(fixture.destination(), &bytes);
    let old = upload(&mut store, original.clone(), &bytes[..20]);
    assert_eq!(
        store
            .begin(operation(), attempt("a"), original.clone())
            .unwrap(),
        Status::Receiving {
            ticket: old.clone(),
            received: 20
        }
    );
    assert!(store.begin(operation(), attempt("b"), original).is_err());
    let mut same_attempt = attempt("a");
    same_attempt.call_id = "another-call".into();
    assert!(store.replace(&old, same_attempt).is_err());
    let current = store.replace(&old, attempt("b")).unwrap();
    assert!(store.append(&old, 0, &bytes).is_err());
    assert!(store.finish(&operation(), &old).is_err());
    assert!(store.cancel(&operation(), &old).is_err());
    store.append(&current, 0, &bytes).unwrap();
    let saved = receipt(store.finish(&operation(), &current).unwrap());
    assert_eq!(saved.attempt, attempt("b"));
}

#[test]
fn cancellation_before_acceptance_fences_publication_and_is_process_local() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let original = intent(fixture.destination(), &bytes);
    let ticket = upload(&mut store, original.clone(), &bytes);
    let aborted = receipt(store.cancel(&operation(), &ticket).unwrap());
    assert_eq!(aborted.outcome, Outcome::Aborted);
    assert!(aborted.accepted_at.is_none());
    assert!(fixture.records().is_empty());
    assert!(fixture.stages().is_empty());
    assert!(!fixture.destination().exists());
    assert_eq!(
        receipt(store.finish(&operation(), &ticket).unwrap()),
        aborted
    );
    assert_eq!(
        receipt(store.begin(operation(), attempt("b"), original).unwrap()),
        aborted
    );
    assert!(store.append(&ticket, 0, &bytes).is_err());
    let observations = store.take_observations();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0]["what"], "abort");
    drop(store);
    assert_eq!(fixture.store().lookup(&operation()).unwrap(), None);
}

#[test]
fn accepted_without_publication_stays_unresolved_and_is_never_republished() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let original = intent(fixture.destination(), &bytes);
    let ticket = upload(&mut store, original.clone(), &bytes);
    store.accept(&ticket).unwrap();
    assert_eq!(fixture.records().len(), 1);
    assert_unresolved(store.cancel(&operation(), &ticket).unwrap());
    assert!(store.take_observations().is_empty());
    drop(store);
    let mut store = fixture.store();
    assert_unresolved(store.lookup(&operation()).unwrap().unwrap());
    assert_unresolved(
        store
            .begin(operation(), attempt("retry"), original)
            .unwrap(),
    );
    assert!(!fixture.destination().exists());
    assert_eq!(fixture.stages().len(), 1);
    assert!(store.take_observations().is_empty());
}

#[test]
fn recovery_requires_retained_inode_and_manifest_proof_not_equal_content() {
    for loss in ["stage", "destination", "replacement", "modified", "parent"] {
        let fixture = Fixture::new();
        let mut store = fixture.store();
        let bytes = png_bytes();
        let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
        store.fault = Some(Fault::AfterLink);
        assert!(matches!(
            store.finish(&operation(), &ticket).unwrap(),
            Status::Unresolved {
                published: Some(true),
                ..
            }
        ));
        drop(store);
        match loss {
            "stage" => fs::remove_file(&fixture.stages()[0]).unwrap(),
            "destination" => fs::remove_file(fixture.destination()).unwrap(),
            "replacement" => {
                fs::remove_file(fixture.destination()).unwrap();
                fs::write(fixture.destination(), &bytes).unwrap();
            }
            "modified" => fs::write(fixture.destination(), b"changed inode contents").unwrap(),
            _ => {
                fs::rename(fixture.home.join("pane/test"), fixture.home.join("moved")).unwrap();
                fs::create_dir(fixture.home.join("pane/test")).unwrap();
            }
        }
        let mut store = fixture.store();
        assert!(matches!(
            store.lookup(&operation()).unwrap().unwrap(),
            Status::Unresolved {
                published: None,
                ..
            }
        ));
        assert!(store.take_observations().is_empty());
    }
}

#[test]
fn cancellation_after_link_returns_recovered_store_receipt() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
    store.fault = Some(Fault::AfterLink);
    assert_unresolved(store.finish(&operation(), &ticket).unwrap());
    assert_eq!(
        receipt(store.cancel(&operation(), &ticket).unwrap()).outcome,
        Outcome::Stored
    );
    assert_eq!(store.take_observations().len(), 1);
}

#[test]
fn acceptance_journal_uncertainty_never_reports_failure_or_publishes() {
    for fault in [Fault::BeforeJournalRename, Fault::AfterJournalRename] {
        let fixture = Fixture::new();
        let mut store = fixture.store();
        let bytes = png_bytes();
        let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
        store.fault = Some(fault);
        assert_unresolved(store.finish(&operation(), &ticket).unwrap());
        assert_unresolved(store.cancel(&operation(), &ticket).unwrap());
        assert!(store.poisoned.is_some());
        assert!(!fixture.destination().exists());
        assert!(store.take_observations().is_empty());
        drop(store);
        let mut store = fixture.store();
        if fault == Fault::BeforeJournalRename {
            assert_eq!(store.lookup(&operation()).unwrap(), None);
        } else {
            assert_unresolved(store.lookup(&operation()).unwrap().unwrap());
        }
        assert!(!fixture.destination().exists());
        assert_eq!(fixture.stages().len(), 1);
    }
}

#[test]
fn terminal_journal_uncertainty_recovers_published_artifact_without_false_failure() {
    for fault in [Fault::BeforeJournalRename, Fault::AfterJournalRename] {
        let fixture = Fixture::new();
        let mut store = fixture.store();
        let bytes = png_bytes();
        let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
        store.accept(&ticket).unwrap();
        store.fault = Some(fault);
        store.publish_accepted(&ticket.key);
        assert!(matches!(
            store.record_status(&ticket.key),
            Status::Unresolved {
                published: Some(true),
                ..
            }
        ));
        assert_eq!(fs::read(fixture.destination()).unwrap(), bytes);
        assert_eq!(fixture.stages().len(), 1);
        assert!(store.take_observations().is_empty());
        let renamed = if fault == Fault::AfterJournalRename {
            Some(
                serde_json::from_slice::<Record>(&fs::read(&fixture.records()[0]).unwrap())
                    .unwrap()
                    .terminal
                    .unwrap(),
            )
        } else {
            None
        };
        drop(store);
        let mut store = fixture.store();
        let saved = receipt(store.lookup(&operation()).unwrap().unwrap());
        assert_eq!(saved.outcome, Outcome::Stored);
        if let Some(renamed) = renamed {
            assert_eq!(saved, renamed);
        }
        assert!(fixture.stages().is_empty());
        assert_eq!(
            store.take_observations().len(),
            usize::from(fault == Fault::BeforeJournalRename)
        );
    }
}

#[test]
fn directory_sync_failure_preserves_published_disposition_and_cause() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
    store.fault = Some(Fault::ParentSync);
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    assert!(matches!(
        saved.outcome,
        Outcome::PublishedDurabilityUnconfirmed { .. }
    ));
    assert_eq!(fs::read(fixture.destination()).unwrap(), bytes);
    let observations = store.take_observations();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0]["what"], "fail_sync");
    assert_eq!(observations[0]["with"]["durability"], "unconfirmed");
    assert_eq!(observations[0]["with"]["published"], true);
    assert!(observations[0]["why"]
        .as_str()
        .unwrap()
        .contains("ParentSync"));
    drop(store);
    assert_eq!(
        receipt(fixture.store().lookup(&operation()).unwrap().unwrap()),
        saved
    );
}

#[test]
fn cleanup_failure_does_not_change_terminal_receipt_and_reopen_retries_cleanup() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
    store.fault = Some(Fault::Cleanup);
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    assert_eq!(saved.outcome, Outcome::Stored);
    assert_eq!(fixture.stages().len(), 1);
    let observations = store.take_observations();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[1]["what"], "fail_unlink");
    assert!(observations[1].get("whom").is_none());
    drop(store);
    let mut store = fixture.store();
    assert_eq!(receipt(store.lookup(&operation()).unwrap().unwrap()), saved);
    assert!(fixture.stages().is_empty());
    assert!(store.take_observations().is_empty());
}

#[test]
fn capability_lock_is_exclusive_and_corruption_fails_only_store_open() {
    let fixture = Fixture::new();
    let store = fixture.store();
    assert!(Store::open(&fixture.home).is_err());
    drop(store);
    let store = fixture.store();
    drop(store);
    fs::write(
        fixture.journal().join(format!("{}.json", "0".repeat(64))),
        b"not json",
    )
    .unwrap();
    assert!(Store::open(&fixture.home).is_err());
    fs::write(fixture.home.join("unrelated-tool-output"), b"available").unwrap();
}

#[test]
fn cleanup_sync_failure_does_not_claim_that_unlink_failed() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let ticket = upload(&mut store, intent(fixture.destination(), &bytes), &bytes);
    store.fault = Some(Fault::CleanupSync);
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    assert_eq!(saved.outcome, Outcome::Stored);
    assert!(fixture.stages().is_empty());
    let observations = store.take_observations();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[1]["what"], "fail_sync");
    assert_eq!(observations[1]["with"]["unlinked"], true);
    assert_eq!(receipt(store.lookup(&operation()).unwrap().unwrap()), saved);
}

#[test]
fn frozen_writer_still_serves_existing_receipts_and_foreign_tickets_are_fenced() {
    let fixture = Fixture::new();
    let mut store = fixture.store();
    let bytes = png_bytes();
    let original = intent(fixture.destination(), &bytes);
    let ticket = upload(&mut store, original.clone(), &bytes);
    let foreign = Fixture::new();
    let mut other = foreign.store();
    let other_ticket = upload(&mut other, intent(foreign.destination(), &bytes), &bytes);
    assert!(store
        .append(&other_ticket, bytes.len() as u64, &[0])
        .is_err());
    assert!(store.cancel(&operation(), &other_ticket).is_err());
    assert!(store.finish(&operation(), &other_ticket).is_err());
    let saved = receipt(store.finish(&operation(), &ticket).unwrap());
    let mut op = operation();
    op.operation_id = "second-save".into();
    let next = ticket_for(
        &mut store,
        op.clone(),
        intent(fixture.destination().with_file_name("second.png"), &bytes),
    );
    store.append(&next, 0, &bytes).unwrap();
    store.fault = Some(Fault::BeforeJournalRename);
    assert_unresolved(store.finish(&op, &next).unwrap());
    assert_eq!(
        receipt(
            store
                .begin(operation(), attempt("retry"), original)
                .unwrap()
        ),
        saved
    );
    assert_eq!(receipt(store.lookup(&operation()).unwrap().unwrap()), saved);
}

fn ticket_for(store: &mut Store, operation: OperationId, intent: Intent) -> Ticket {
    ticket(store.begin(operation, attempt("a"), intent).unwrap())
}

#[test]
fn process_exit_at_receiving_acceptance_link_and_terminal_boundaries() {
    for boundary in ["receiving", "accepted", "linked", "terminal"] {
        let fixture = Fixture::new();
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "screenshot::tests::crash_child", "--nocapture"])
            .env("WICKET_SCREENSHOT_CRASH_HOME", &fixture.home)
            .env("WICKET_SCREENSHOT_CRASH_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{boundary}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let mut store = fixture.store();
        let status = store.lookup(&operation()).unwrap();
        match boundary {
            "receiving" => {
                assert_eq!(status, None);
                assert_eq!(fixture.stages().len(), 1);
            }
            "accepted" => {
                assert_unresolved(status.unwrap());
                assert!(!fixture.destination().exists());
            }
            _ => {
                assert_eq!(receipt(status.unwrap()).outcome, Outcome::Stored);
                assert_eq!(fs::read(fixture.destination()).unwrap(), png_bytes());
                assert!(fixture.stages().is_empty());
            }
        }
    }
}

#[test]
fn crash_child() {
    let Some(home) = std::env::var_os("WICKET_SCREENSHOT_CRASH_HOME") else {
        return;
    };
    let home = PathBuf::from(home);
    let boundary = std::env::var("WICKET_SCREENSHOT_CRASH_BOUNDARY").unwrap();
    let mut store = Store::open(&home).unwrap();
    let bytes = png_bytes();
    let ticket = upload(
        &mut store,
        intent(home.join("pane/test/capture.png"), &bytes),
        &bytes,
    );
    if boundary != "receiving" {
        store.accept(&ticket).unwrap();
    }
    if boundary == "linked" {
        store.fault = Some(Fault::AfterLink);
        store.publish_accepted(&ticket.key);
        assert!(matches!(
            store.record_status(&ticket.key),
            Status::Unresolved {
                published: Some(true),
                ..
            }
        ));
    }
    if boundary == "terminal" {
        store.fault = Some(Fault::Cleanup);
        store.publish_accepted(&ticket.key);
        assert_eq!(
            receipt(store.record_status(&ticket.key)).outcome,
            Outcome::Stored
        );
    }
    // Skip Store::drop: exercise actual process loss with OS lock release.
    std::process::exit(0);
}
