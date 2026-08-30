use flatbuffers_build::BuilderOptions;

fn main() {
    BuilderOptions::new_with_files(["schemas/signalweave_v1.fbs"])
        .set_compiler(
            flatc_fork::flatc()
                .to_str()
                .expect("vendored flatc path must be valid UTF-8"),
        )
        .compile()
        .expect("failed to generate Signalweave Protocol FlatBuffers bindings");
}
