#[path = "../tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::{fs, path::PathBuf};

use signalweave_protocol::Codec;

fn main() {
    let bytes = Codec::default()
        .encode(&common::tool_call_completed_envelope())
        .expect("tool_call_completed envelope must encode");
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tool_call_completed_v1.swp");
    fs::write(&path, bytes).expect("fixture must be writable");
    println!("wrote {}", path.display());
}
