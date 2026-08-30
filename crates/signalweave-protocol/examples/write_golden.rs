#[path = "../tests/common/mod.rs"]
mod common;

use std::{fs, path::PathBuf};

use signalweave_protocol::Codec;

fn main() {
    let bytes = Codec::default()
        .encode(&common::golden_envelope())
        .expect("golden envelope must encode");
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/reliable_event_v1.swp");
    fs::write(&path, bytes).expect("golden fixture must be writable");
    println!("wrote {}", path.display());
}
