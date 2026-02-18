#[cfg(feature = "wasm-client")]
use wasm_bindgen::prelude::*;

pub const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-BACKEND");

#[cfg(feature = "wasm-client")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"])]
    pub async fn invoke(cmd: &str, args: JsValue) -> JsValue;
}

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[cfg(all(feature = "backend", not(target_arch = "wasm32")))]
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[cfg(all(feature = "backend", not(target_arch = "wasm32")))]
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
