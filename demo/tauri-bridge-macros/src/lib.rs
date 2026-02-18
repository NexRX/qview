//! Tauri Bridge Macros
//!
//! This crate provides the `#[tauri_bridge]` proc macro that generates both
//! backend Tauri commands and WASM client bindings from a single function definition.
//!
//! # Example
//!
//! ```rust,ignore
//! #[tauri_bridge]
//! pub fn greet(name: &str) -> String {
//!     format!("Hello, {}!", name)
//! }
//! ```
//!
//! This generates:
//! - On backend: A `#[tauri::command]` function
//! - On WASM client:
//!   - A `GreetArgs` struct with Serialize/Deserialize
//!   - `try_call_greet` async function that returns `Result<T, String>`
//!   - `call_greet` async function that unwraps the result

mod backend;
mod client;
mod types;

#[cfg(test)]
mod tests;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote_spanned;
use syn::{parse_macro_input, ItemFn};

use backend::generate_backend;
use client::generate_client;

/// Macro that generates both backend Tauri command and WASM client bindings.
///
/// # Example
///
/// ```rust,ignore
/// #[tauri_bridge]
/// pub fn greet(name: &str) -> String {
///     format!("Hello, {}!", name)
/// }
/// ```
///
/// This generates:
/// - On backend: A `#[tauri::command]` function
/// - On WASM client:
///   - A `GreetArgs` struct
///   - `try_call_greet` async function that returns `Result<T, String>`
///   - `call_greet` async function that unwraps the result
#[proc_macro_attribute]
pub fn tauri_bridge(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let backend_code = generate_backend(&input);
    let client_code = generate_client(&input);

    let call_site = Span::call_site();

    let expanded = quote_spanned! {call_site=>
        #backend_code
        #client_code
    };

    TokenStream::from(expanded)
}
