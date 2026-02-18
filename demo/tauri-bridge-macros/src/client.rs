//! WASM client code generation for Tauri command bindings.

use convert_case::{Case, Casing};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote_spanned;
use syn::{FnArg, ItemFn, Pat};

use crate::types::{
    generate_try_deserialize_expr, get_return_type, has_reference_type, transform_ref_to_lifetime,
};

/// Generate client-side code for WASM.
///
/// This generates:
/// - An args struct with Serialize/Deserialize derives
/// - A `try_call_<name>` async function returning `Result<T, String>`
/// - A `call_<name>` async function that unwraps the result
pub fn generate_client(input: &ItemFn) -> TokenStream2 {
    let fn_name = &input.sig.ident;
    let fn_name_str = fn_name.to_string();
    let vis = &input.vis;
    let call_site = Span::call_site();

    // Generate args struct name (e.g., greet -> GreetArgs)
    let args_struct_name = syn::Ident::new(
        &format!("{}Args", fn_name_str.to_case(Case::Pascal)),
        call_site,
    );

    // Generate client function names
    let try_call_fn_name = syn::Ident::new(&format!("try_call_{}", fn_name), call_site);
    let call_fn_name = syn::Ident::new(&format!("call_{}", fn_name), call_site);

    // Extract function arguments
    let args: Vec<_> = input
        .sig
        .inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                Some(pat_type)
            } else {
                None
            }
        })
        .collect();

    // Check if we have any arguments
    let has_args = !args.is_empty();

    // Check if any argument has a reference type (needs lifetime)
    let needs_lifetime = args.iter().any(|arg| has_reference_type(&arg.ty));

    // Generate struct fields with proper lifetime handling
    let struct_fields: Vec<_> = args
        .iter()
        .map(|pat_type| {
            let pat = &pat_type.pat;
            let ty = if needs_lifetime {
                transform_ref_to_lifetime(&pat_type.ty, call_site)
            } else {
                quote_spanned! {call_site=> #pat_type.ty }
            };
            quote_spanned! {call_site=> #pat: #ty }
        })
        .collect();

    // Generate function parameters with proper lifetime handling
    let fn_params: Vec<_> = args
        .iter()
        .map(|pat_type| {
            let pat = &pat_type.pat;
            let ty = if needs_lifetime {
                transform_ref_to_lifetime(&pat_type.ty, call_site)
            } else {
                let ty = &pat_type.ty;
                quote_spanned! {call_site=> #ty }
            };
            quote_spanned! {call_site=> #pat: #ty }
        })
        .collect();

    // Generate struct field initializers
    let field_inits: Vec<_> = args
        .iter()
        .filter_map(|pat_type| {
            if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                let ident = syn::Ident::new(&pat_ident.ident.to_string(), call_site);
                Some(quote_spanned! {call_site=> #ident })
            } else {
                None
            }
        })
        .collect();

    // Generate argument forwarding for call_ -> try_call_
    let arg_forwards: Vec<_> = args
        .iter()
        .filter_map(|pat_type| {
            if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                let ident = syn::Ident::new(&pat_ident.ident.to_string(), call_site);
                Some(quote_spanned! {call_site=> #ident })
            } else {
                None
            }
        })
        .collect();

    // Get return type
    let return_type = get_return_type(&input.sig);
    let try_deserialize_expr = generate_try_deserialize_expr(&return_type, call_site);

    // Generate the struct definition with appropriate lifetime
    let struct_def = if has_args {
        if needs_lifetime {
            quote_spanned! {call_site=>
                #[cfg(feature = "wasm-client")]
                #[derive(serde::Serialize, serde::Deserialize)]
                struct #args_struct_name<'a> {
                    #(#struct_fields),*
                }
            }
        } else {
            quote_spanned! {call_site=>
                #[cfg(feature = "wasm-client")]
                #[derive(serde::Serialize, serde::Deserialize)]
                struct #args_struct_name {
                    #(#struct_fields),*
                }
            }
        }
    } else {
        quote_spanned! {call_site=> }
    };

    // Generate the invoke call for try_call (returns Result)
    let try_invoke_call = if has_args {
        quote_spanned! {call_site=>
            let args = serde_wasm_bindgen::to_value(&#args_struct_name { #(#field_inits),* })
                .map_err(|e| format!("Failed to serialize arguments: {}", e))?;
            let result = crate::invoke(#fn_name_str, args).await;
        }
    } else {
        quote_spanned! {call_site=>
            let args = serde_wasm_bindgen::to_value(&serde_json::Value::Null)
                .map_err(|e| format!("Failed to serialize arguments: {}", e))?;
            let result = crate::invoke(#fn_name_str, args).await;
        }
    };

    // Generate both try_call_ and call_ functions
    let client_fns = if needs_lifetime {
        quote_spanned! {call_site=>
            #[cfg(feature = "wasm-client")]
            #vis async fn #try_call_fn_name<'a>(#(#fn_params),*) -> Result<#return_type, String> {
                #try_invoke_call
                #try_deserialize_expr
            }

            #[cfg(feature = "wasm-client")]
            #vis async fn #call_fn_name<'a>(#(#fn_params),*) -> #return_type {
                #try_call_fn_name(#(#arg_forwards),*).await.unwrap()
            }
        }
    } else {
        quote_spanned! {call_site=>
            #[cfg(feature = "wasm-client")]
            #vis async fn #try_call_fn_name(#(#fn_params),*) -> Result<#return_type, String> {
                #try_invoke_call
                #try_deserialize_expr
            }

            #[cfg(feature = "wasm-client")]
            #vis async fn #call_fn_name(#(#fn_params),*) -> #return_type {
                #try_call_fn_name(#(#arg_forwards),*).await.unwrap()
            }
        }
    };

    quote_spanned! {call_site=>
        #struct_def
        #client_fns
    }
}
