//! Guards the acceptance criterion of the transport seam: no stage may name a
//! transport-specific type.
//!
//! This is a lint, not a behaviour test. The seam is only worth anything if it
//! is actually used, and the failure mode is quiet — someone reaches for an
//! `mpsc::Sender` directly because it is one line shorter, and M5 turns back
//! into a rewrite. A compiler cannot catch that, so the check is here.
//!
//! It fails loudly with the offending file and line, and it names the single
//! file that is allowed to know.

use std::{fs, path::Path};

/// Transport implementations live here and nowhere else.
const ALLOWED: &[&str] = &["crates/core/src/transport.rs"];

/// Spellings that mean "I am talking to a specific transport".
const FORBIDDEN: &[&str] = &["mpsc", "tokio::sync::channel", "rdkafka", "kafka::"];

#[test]
fn no_stage_names_a_transport_type() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root");

    let mut offences = Vec::new();
    for dir in ["crates", "bins"] {
        walk(&root.join(dir), &root, &mut offences);
    }

    assert!(
        offences.is_empty(),
        "transport details leaked outside {}:\n{}\n\nStages must go through \
         EventSink and EventSource. If a new transport implementation is being \
         added, add its file to ALLOWED in this test.",
        ALLOWED.join(", "),
        offences.join("\n")
    );
}

fn walk(dir: &Path, root: &Path, offences: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            walk(&path, root, offences);
            continue;
        }

        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }

        let rel = path.strip_prefix(root).unwrap_or(&path).to_string_lossy().replace('\\', "/");
        if ALLOWED.contains(&rel.as_str()) || rel.contains("/tests/") {
            continue;
        }

        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            // Prose in a doc comment explaining the design is fine; code is not.
            let code = line.trim_start();
            if code.starts_with("//") {
                continue;
            }
            for needle in FORBIDDEN {
                if code.contains(needle) {
                    offences.push(format!("  {rel}:{}: {}", i + 1, line.trim()));
                }
            }
        }
    }
}
