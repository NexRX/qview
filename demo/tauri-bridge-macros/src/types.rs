//! Type analysis utilities for reference detection and lifetime transformation.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote_spanned;
use syn::{ReturnType, Signature, Type};

/// Check if a type contains any references (including nested in generics).
pub fn has_reference_type(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Path(type_path) => {
            // Check generic arguments for references
            if let Some(segment) = type_path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    return args.args.iter().any(|arg| {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            has_reference_type(inner_ty)
                        } else {
                            false
                        }
                    });
                }
            }
            false
        }
        Type::Tuple(tuple) => tuple.elems.iter().any(has_reference_type),
        Type::Array(array) => has_reference_type(&array.elem),
        Type::Slice(slice) => has_reference_type(&slice.elem),
        Type::Paren(paren) => has_reference_type(&paren.elem),
        _ => false,
    }
}

/// Transform reference types to use explicit `'a` lifetime.
///
/// This recursively transforms types like `&str` to `&'a str`,
/// while preserving existing explicit lifetimes like `&'static str`.
pub fn transform_ref_to_lifetime(ty: &Type, span: Span) -> TokenStream2 {
    match ty {
        Type::Reference(type_ref) => {
            let mutability = &type_ref.mutability;
            let elem = transform_ref_to_lifetime(&type_ref.elem, span);
            if type_ref.lifetime.is_some() {
                // Already has a lifetime, keep it
                let lifetime = &type_ref.lifetime;
                quote_spanned! {span=> &#lifetime #mutability #elem }
            } else {
                // Add 'a lifetime
                quote_spanned! {span=> &'a #mutability #elem }
            }
        }
        Type::Path(type_path) => {
            // Handle generic arguments that might contain references
            if let Some(segment) = type_path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    let path_without_last = type_path
                        .path
                        .segments
                        .iter()
                        .take(type_path.path.segments.len() - 1);
                    let last_ident = &segment.ident;
                    let transformed_args: Vec<_> = args
                        .args
                        .iter()
                        .map(|arg| {
                            if let syn::GenericArgument::Type(inner_ty) = arg {
                                let transformed = transform_ref_to_lifetime(inner_ty, span);
                                quote_spanned! {span=> #transformed }
                            } else {
                                quote_spanned! {span=> #arg }
                            }
                        })
                        .collect();

                    let prefix: Vec<_> = path_without_last.collect();
                    if prefix.is_empty() {
                        return quote_spanned! {span=> #last_ident<#(#transformed_args),*> };
                    } else {
                        return quote_spanned! {span=> #(#prefix::)* #last_ident<#(#transformed_args),*> };
                    }
                }
            }
            quote_spanned! {span=> #type_path }
        }
        Type::Tuple(tuple) => {
            let transformed: Vec<_> = tuple
                .elems
                .iter()
                .map(|elem| transform_ref_to_lifetime(elem, span))
                .collect();
            quote_spanned! {span=> (#(#transformed),*) }
        }
        Type::Array(array) => {
            let elem = transform_ref_to_lifetime(&array.elem, span);
            let len = &array.len;
            quote_spanned! {span=> [#elem; #len] }
        }
        Type::Slice(slice) => {
            let elem = transform_ref_to_lifetime(&slice.elem, span);
            quote_spanned! {span=> [#elem] }
        }
        Type::Paren(paren) => {
            let elem = transform_ref_to_lifetime(&paren.elem, span);
            quote_spanned! {span=> (#elem) }
        }
        _ => {
            quote_spanned! {span=> #ty }
        }
    }
}

/// Extract the return type from a function signature.
pub fn get_return_type(sig: &Signature) -> TokenStream2 {
    let call_site = Span::call_site();
    match &sig.output {
        ReturnType::Default => quote_spanned! {call_site=> () },
        ReturnType::Type(_, ty) => quote_spanned! {call_site=> #ty },
    }
}

/// Generate deserialize expression that returns Result.
///
/// Different return types need different deserialization strategies:
/// - `String`: uses `as_string()`
/// - `bool`: uses `as_bool()`
/// - Numeric types: uses `serde_wasm_bindgen::from_value`
/// - Complex types: uses `serde_wasm_bindgen::from_value`
pub fn generate_try_deserialize_expr(return_type: &TokenStream2, span: Span) -> TokenStream2 {
    let type_str = return_type.to_string();

    // Handle common types with specialized deserialization
    if type_str == "String" {
        quote_spanned! {span=>
            result.as_string().ok_or_else(|| "Expected string response".to_string())
        }
    } else if type_str == "()" {
        quote_spanned! {span=>
            Ok(())
        }
    } else if type_str == "bool" {
        quote_spanned! {span=>
            result.as_bool().ok_or_else(|| "Expected bool response".to_string())
        }
    } else if type_str == "i32"
        || type_str == "i64"
        || type_str == "u32"
        || type_str == "u64"
        || type_str == "f32"
        || type_str == "f64"
        || type_str == "isize"
        || type_str == "usize"
    {
        quote_spanned! {span=>
            serde_wasm_bindgen::from_value(result)
                .map_err(|e| format!("Failed to deserialize number: {}", e))
        }
    } else {
        // For complex types, use serde_wasm_bindgen
        quote_spanned! {span=>
            serde_wasm_bindgen::from_value(result)
                .map_err(|e| format!("Failed to deserialize response: {}", e))
        }
    }
}
