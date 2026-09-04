use std::{env, fs, path::PathBuf};

use flatbuffers_build::BuilderOptions;

fn vendored_flatc() -> PathBuf {
    let configured = flatc_fork::flatc();
    if configured.is_file() {
        return configured.to_owned();
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo must provide OUT_DIR"));
    let build_dir = out_dir
        .ancestors()
        .nth(2)
        .expect("Cargo OUT_DIR must be nested below target/*/build");

    fs::read_dir(build_dir)
        .expect("Cargo build directory must be readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("out/bin/flatc"))
        .find(|candidate| {
            candidate.is_file()
                && candidate
                    .ancestors()
                    .nth(3)
                    .and_then(|directory| directory.file_name())
                    .is_some_and(|name| name.to_string_lossy().starts_with("flatc-fork-"))
        })
        .expect("vendored flatc must be built before Woven Protocol")
}

fn main() {
    let compiler = vendored_flatc();
    BuilderOptions::new_with_files(["schemas/woven_v1.fbs"])
        .set_compiler(
            compiler
                .to_str()
                .expect("vendored flatc path must be valid UTF-8"),
        )
        .compile()
        .expect("failed to generate Woven Protocol FlatBuffers bindings");
}
