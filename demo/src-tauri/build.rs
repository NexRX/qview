fn main() {
    // Build scripts run on the host, so cfg(target_arch) checks the host, not the target.
    // We need to check the TARGET environment variable to know what we're compiling for.
    let target = std::env::var("TARGET").unwrap_or_default();

    if !target.contains("wasm") {
        tauri_build::build()
    }
}
