//! Backend code generation for Tauri commands.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote_spanned;
use syn::ItemFn;

/// Generate backend code with `#[tauri::command]` attribute.
///
/// The generated code wraps the function in a module to isolate
/// the macro exports from `#[tauri::command]`.
pub fn generate_backend(input: &ItemFn) -> TokenStream2 {
    let vis = &input.vis;
    let fn_name = &input.sig.ident;
    let fn_name_str = fn_name.to_string();
    let inputs = &input.sig.inputs;
    let output = &input.sig.output;
    let block = &input.block;
    let attrs = &input.attrs;
    let asyncness = &input.sig.asyncness;
    let generics = &input.sig.generics;
    let where_clause = &input.sig.generics.where_clause;

    let call_site = Span::call_site();

    // Create a unique module name to isolate the tauri::command macro's exports
    let mod_name = syn::Ident::new(&format!("__tauri_cmd_{}", fn_name_str), call_site);
    let fn_name_new = syn::Ident::new(&fn_name_str, call_site);

    quote_spanned! {call_site=>
        #[cfg(all(feature = "backend", not(target_arch = "wasm32")))]
        mod #mod_name {
            use super::*;

            #(#attrs)*
            #[tauri::command]
            #vis #asyncness fn #fn_name_new #generics (#inputs) #output #where_clause #block
        }

        #[cfg(all(feature = "backend", not(target_arch = "wasm32")))]
        #vis use #mod_name::#fn_name_new;
    }
}
