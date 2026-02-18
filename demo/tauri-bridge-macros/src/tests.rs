//! Unit tests for the tauri-bridge-macros crate.

use proc_macro2::{Span, TokenStream as TokenStream2};
use syn::{parse_quote, ItemFn, Signature, Type};

use crate::backend::generate_backend;
use crate::client::generate_client;
use crate::types::{get_return_type, has_reference_type, transform_ref_to_lifetime};

/// Helper to normalize whitespace for comparison
fn normalize_tokens(tokens: &TokenStream2) -> String {
    tokens
        .to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Helper to check if generated code contains expected patterns
fn contains_pattern(generated: &TokenStream2, pattern: &str) -> bool {
    normalize_tokens(generated).contains(pattern)
}

// ==================== Basic Function Tests ====================

#[test]
fn test_basic_string_return() {
    let input: ItemFn = parse_quote! {
        pub fn greet(name: &str) -> String {
            format!("Hello, {}!", name)
        }
    };

    let backend = generate_backend(&input);
    let client = generate_client(&input);

    // Backend should have #[tauri::command]
    assert!(contains_pattern(&backend, "# [tauri :: command]"));
    // Backend should have the function
    assert!(contains_pattern(&backend, "pub fn greet"));

    // Client should have GreetArgs struct
    assert!(contains_pattern(&client, "struct GreetArgs"));
    // Client should have try_call_greet
    assert!(contains_pattern(&client, "async fn try_call_greet"));
    // Client should have call_greet
    assert!(contains_pattern(&client, "async fn call_greet"));
    // Client should use lifetime for &str
    assert!(contains_pattern(&client, "& 'a str"));
}

#[test]
fn test_no_args_function() {
    let input: ItemFn = parse_quote! {
        pub fn get_version() -> String {
            "1.0.0".to_string()
        }
    };

    let client = generate_client(&input);

    // Should NOT have args struct (no args)
    assert!(!contains_pattern(&client, "struct GetVersionArgs"));
    // Should have the functions
    assert!(contains_pattern(&client, "async fn try_call_get_version"));
    assert!(contains_pattern(&client, "async fn call_get_version"));
}

#[test]
fn test_unit_return_type() {
    let input: ItemFn = parse_quote! {
        pub fn do_something(value: i32) {
            println!("{}", value);
        }
    };

    let client = generate_client(&input);

    // Should return Result<(), String> for try_call
    assert!(contains_pattern(&client, "-> Result < () , String >"));
    // Should have Ok(()) in deserialization
    assert!(contains_pattern(&client, "Ok (())"));
}

// ==================== Multiple Arguments Tests ====================

#[test]
fn test_multiple_args() {
    let input: ItemFn = parse_quote! {
        pub fn add(a: i32, b: i32) -> i32 {
            a + b
        }
    };

    let client = generate_client(&input);

    // Should have AddArgs struct with both fields
    assert!(contains_pattern(&client, "struct AddArgs"));
    assert!(contains_pattern(&client, "a : i32"));
    assert!(contains_pattern(&client, "b : i32"));
    // Function should have both params
    assert!(contains_pattern(&client, "a : i32 , b : i32"));
}

#[test]
fn test_mixed_ref_and_owned_args() {
    let input: ItemFn = parse_quote! {
        pub fn process(name: &str, count: u32, data: &str) -> String {
            format!("{}: {} x {}", name, data, count)
        }
    };

    let client = generate_client(&input);

    // Should have lifetime on struct
    assert!(contains_pattern(&client, "struct ProcessArgs < 'a >"));
    // References should have 'a lifetime
    assert!(contains_pattern(&client, "name : & 'a str"));
    assert!(contains_pattern(&client, "data : & 'a str"));
    // Owned types should remain as-is
    assert!(contains_pattern(&client, "count : u32"));
}

// ==================== Async Function Tests ====================

#[test]
fn test_async_function() {
    let input: ItemFn = parse_quote! {
        pub async fn fetch_data(url: &str) -> String {
            // async impl
            String::new()
        }
    };

    let backend = generate_backend(&input);

    // Backend should preserve async
    assert!(contains_pattern(&backend, "pub async fn fetch_data"));
}

// ==================== Return Type Tests ====================

#[test]
fn test_bool_return() {
    let input: ItemFn = parse_quote! {
        pub fn is_valid(value: i32) -> bool {
            value > 0
        }
    };

    let client = generate_client(&input);

    // Should use as_bool() for deserialization
    assert!(contains_pattern(&client, "result . as_bool ()"));
}

#[test]
fn test_numeric_return_i32() {
    let input: ItemFn = parse_quote! {
        pub fn calculate(x: i32) -> i32 {
            x * 2
        }
    };

    let client = generate_client(&input);

    // Should use serde_wasm_bindgen for numbers
    assert!(contains_pattern(
        &client,
        "serde_wasm_bindgen :: from_value (result)"
    ));
}

#[test]
fn test_complex_return_type() {
    let input: ItemFn = parse_quote! {
        pub fn get_user(id: u64) -> UserData {
            UserData::default()
        }
    };

    let client = generate_client(&input);

    // Should use serde_wasm_bindgen for complex types
    assert!(contains_pattern(
        &client,
        "serde_wasm_bindgen :: from_value (result)"
    ));
    // Return type should be Result<UserData, String>
    assert!(contains_pattern(&client, "-> Result < UserData , String >"));
}

// ==================== Struct/Enum Argument Tests ====================

#[test]
fn test_struct_argument() {
    let input: ItemFn = parse_quote! {
        pub fn save_user(user: UserData) -> bool {
            true
        }
    };

    let client = generate_client(&input);

    // Should have SaveUserArgs with user field
    assert!(contains_pattern(&client, "struct SaveUserArgs"));
    assert!(contains_pattern(&client, "user : UserData"));
    // No lifetime needed (owned type)
    assert!(!contains_pattern(&client, "SaveUserArgs < 'a >"));
}

#[test]
fn test_struct_reference_argument() {
    let input: ItemFn = parse_quote! {
        pub fn validate_user(user: &UserData) -> bool {
            true
        }
    };

    let client = generate_client(&input);

    // Should have lifetime
    assert!(contains_pattern(&client, "struct ValidateUserArgs < 'a >"));
    assert!(contains_pattern(&client, "user : & 'a UserData"));
}

#[test]
fn test_enum_argument() {
    let input: ItemFn = parse_quote! {
        pub fn handle_action(action: Action) -> String {
            String::new()
        }
    };

    let client = generate_client(&input);

    // Should have HandleActionArgs with action field
    assert!(contains_pattern(&client, "struct HandleActionArgs"));
    assert!(contains_pattern(&client, "action : Action"));
}

// ==================== Generic Type Tests ====================

#[test]
fn test_vec_argument() {
    let input: ItemFn = parse_quote! {
        pub fn sum_all(numbers: Vec<i32>) -> i32 {
            numbers.iter().sum()
        }
    };

    let client = generate_client(&input);

    // Should have Vec<i32> in args
    assert!(contains_pattern(&client, "numbers : Vec < i32 >"));
}

#[test]
fn test_option_argument() {
    let input: ItemFn = parse_quote! {
        pub fn maybe_greet(name: Option<String>) -> String {
            name.unwrap_or_default()
        }
    };

    let client = generate_client(&input);

    // Should have Option<String> in args
    assert!(contains_pattern(&client, "name : Option < String >"));
}

#[test]
fn test_vec_with_ref_elements() {
    let input: ItemFn = parse_quote! {
        pub fn join_strings(strings: &[&str]) -> String {
            strings.join(", ")
        }
    };

    let client = generate_client(&input);

    // Should have lifetime
    assert!(contains_pattern(&client, "< 'a >"));
}

// ==================== Edge Cases ====================

#[test]
fn test_private_function() {
    let input: ItemFn = parse_quote! {
        fn internal_helper(x: i32) -> i32 {
            x
        }
    };

    let client = generate_client(&input);

    // Should NOT have pub
    assert!(contains_pattern(
        &client,
        "async fn try_call_internal_helper"
    ));
    assert!(!contains_pattern(
        &client,
        "pub async fn try_call_internal_helper"
    ));
}

#[test]
fn test_function_with_attributes() {
    let input: ItemFn = parse_quote! {
        #[doc = "This is a documented function"]
        #[inline]
        pub fn documented(x: i32) -> i32 {
            x
        }
    };

    let backend = generate_backend(&input);

    // Should preserve attributes
    assert!(contains_pattern(
        &backend,
        "# [doc = \"This is a documented function\"]"
    ));
    assert!(contains_pattern(&backend, "# [inline]"));
}

#[test]
fn test_snake_case_to_pascal_case() {
    let input: ItemFn = parse_quote! {
        pub fn get_user_data(user_id: u64) -> String {
            String::new()
        }
    };

    let client = generate_client(&input);

    // Should convert get_user_data to GetUserDataArgs
    assert!(contains_pattern(&client, "struct GetUserDataArgs"));
}

#[test]
fn test_mutable_reference() {
    let input: ItemFn = parse_quote! {
        pub fn modify_buffer(buf: &mut [u8]) -> usize {
            buf.len()
        }
    };

    let client = generate_client(&input);

    // Should preserve mut
    assert!(contains_pattern(&client, "& 'a mut"));
}

// ==================== Result Return Type Tests ====================

#[test]
fn test_result_return_type() {
    let input: ItemFn = parse_quote! {
        pub fn fallible_op(x: i32) -> Result<String, Error> {
            Ok(x.to_string())
        }
    };

    let client = generate_client(&input);

    // The outer wrapper should be Result<Result<String, Error>, String>
    assert!(contains_pattern(
        &client,
        "Result < Result < String , Error > , String >"
    ));
}

// ==================== Helper Function Tests ====================

#[test]
fn test_has_reference_type_simple_ref() {
    let ty: Type = parse_quote!(&str);
    assert!(has_reference_type(&ty));
}

#[test]
fn test_has_reference_type_owned() {
    let ty: Type = parse_quote!(String);
    assert!(!has_reference_type(&ty));
}

#[test]
fn test_has_reference_type_vec_of_refs() {
    let ty: Type = parse_quote!(Vec<&str>);
    assert!(has_reference_type(&ty));
}

#[test]
fn test_has_reference_type_tuple_with_ref() {
    let ty: Type = parse_quote!((i32, &str, bool));
    assert!(has_reference_type(&ty));
}

#[test]
fn test_has_reference_type_array() {
    let ty: Type = parse_quote!([&str; 3]);
    assert!(has_reference_type(&ty));
}

#[test]
fn test_transform_ref_to_lifetime() {
    let ty: Type = parse_quote!(&str);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("& 'a str"));
}

#[test]
fn test_transform_preserves_existing_lifetime() {
    let ty: Type = parse_quote!(&'static str);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    // Should keep 'static, not replace with 'a
    assert!(normalize_tokens(&transformed).contains("'static"));
}

#[test]
fn test_get_return_type_explicit() {
    let sig: Signature = parse_quote!(fn foo() -> String);
    let ret = get_return_type(&sig);
    assert_eq!(normalize_tokens(&ret), "String");
}

#[test]
fn test_get_return_type_unit() {
    let sig: Signature = parse_quote!(fn foo());
    let ret = get_return_type(&sig);
    assert_eq!(normalize_tokens(&ret), "()");
}

// ==================== Reference Handling Tests ====================

#[test]
fn test_ref_str() {
    let input: ItemFn = parse_quote! {
        pub fn takes_str(s: &str) -> String {
            s.to_string()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesStrArgs < 'a >"));
    assert!(contains_pattern(&client, "s : & 'a str"));
}

#[test]
fn test_ref_string() {
    let input: ItemFn = parse_quote! {
        pub fn takes_ref_string(s: &String) -> String {
            s.clone()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(
        &client,
        "struct TakesRefStringArgs < 'a >"
    ));
    assert!(contains_pattern(&client, "s : & 'a String"));
}

#[test]
fn test_ref_slice_u8() {
    let input: ItemFn = parse_quote! {
        pub fn takes_bytes(data: &[u8]) -> usize {
            data.len()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesBytesArgs < 'a >"));
    assert!(contains_pattern(&client, "data : & 'a [u8]"));
}

#[test]
fn test_ref_slice_of_refs() {
    let input: ItemFn = parse_quote! {
        pub fn takes_str_slice(items: &[&str]) -> String {
            items.join(",")
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesStrSliceArgs < 'a >"));
    assert!(contains_pattern(&client, "& 'a [& 'a str]"));
}

#[test]
fn test_ref_mut_slice() {
    let input: ItemFn = parse_quote! {
        pub fn modify_bytes(data: &mut [u8]) -> usize {
            data.len()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "& 'a mut [u8]"));
}

#[test]
fn test_ref_mut_str() {
    let input: ItemFn = parse_quote! {
        pub fn modify_str(s: &mut str) -> () {
            ()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "& 'a mut str"));
}

#[test]
fn test_ref_custom_struct() {
    let input: ItemFn = parse_quote! {
        pub fn takes_user_ref(user: &User) -> String {
            user.name.clone()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesUserRefArgs < 'a >"));
    assert!(contains_pattern(&client, "user : & 'a User"));
}

#[test]
fn test_ref_with_explicit_lifetime() {
    let input: ItemFn = parse_quote! {
        pub fn takes_static(s: &'static str) -> String {
            s.to_string()
        }
    };
    let client = generate_client(&input);
    // Should preserve 'static, not replace with 'a
    assert!(contains_pattern(&client, "& 'static str"));
}

#[test]
fn test_option_of_ref() {
    let input: ItemFn = parse_quote! {
        pub fn maybe_str(s: Option<&str>) -> String {
            s.unwrap_or("").to_string()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct MaybeStrArgs < 'a >"));
    assert!(contains_pattern(&client, "Option < & 'a str >"));
}

#[test]
fn test_vec_of_refs() {
    let input: ItemFn = parse_quote! {
        pub fn takes_vec_refs(items: Vec<&str>) -> String {
            items.join(",")
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesVecRefsArgs < 'a >"));
    assert!(contains_pattern(&client, "Vec < & 'a str >"));
}

#[test]
fn test_hashmap_with_ref_key() {
    let input: ItemFn = parse_quote! {
        pub fn takes_map(map: std::collections::HashMap<&str, i32>) -> i32 {
            0
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "< 'a >"));
    assert!(contains_pattern(&client, "& 'a str"));
}

#[test]
fn test_tuple_with_refs() {
    let input: ItemFn = parse_quote! {
        pub fn takes_tuple(t: (&str, &str)) -> String {
            format!("{}{}", t.0, t.1)
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesTupleArgs < 'a >"));
    assert!(contains_pattern(&client, "(& 'a str , & 'a str)"));
}

#[test]
fn test_nested_refs_in_option_vec() {
    let input: ItemFn = parse_quote! {
        pub fn complex_refs(data: Option<Vec<&str>>) -> usize {
            data.map(|v| v.len()).unwrap_or(0)
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct ComplexRefsArgs < 'a >"));
    assert!(contains_pattern(&client, "Option < Vec < & 'a str > >"));
}

#[test]
fn test_array_of_refs() {
    let input: ItemFn = parse_quote! {
        pub fn takes_array(arr: [&str; 3]) -> String {
            arr.join("")
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesArrayArgs < 'a >"));
    assert!(contains_pattern(&client, "[& 'a str ; 3]"));
}

#[test]
fn test_ref_to_array() {
    let input: ItemFn = parse_quote! {
        pub fn takes_ref_array(arr: &[i32; 5]) -> i32 {
            arr.iter().sum()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "& 'a [i32 ; 5]"));
}

#[test]
fn test_double_ref() {
    let input: ItemFn = parse_quote! {
        pub fn takes_double_ref(s: &&str) -> String {
            s.to_string()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "& 'a & 'a str"));
}

#[test]
fn test_ref_in_result() {
    let input: ItemFn = parse_quote! {
        pub fn takes_result_ref(r: Result<&str, &str>) -> String {
            r.unwrap_or("").to_string()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(
        &client,
        "struct TakesResultRefArgs < 'a >"
    ));
    assert!(contains_pattern(&client, "Result < & 'a str , & 'a str >"));
}

#[test]
fn test_multiple_ref_args() {
    let input: ItemFn = parse_quote! {
        pub fn concat_all(a: &str, b: &str, c: &str) -> String {
            format!("{}{}{}", a, b, c)
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct ConcatAllArgs < 'a >"));
    assert!(contains_pattern(&client, "a : & 'a str"));
    assert!(contains_pattern(&client, "b : & 'a str"));
    assert!(contains_pattern(&client, "c : & 'a str"));
}

#[test]
fn test_mixed_ref_and_owned_multiple() {
    let input: ItemFn = parse_quote! {
        pub fn mixed(name: &str, count: u32, data: Vec<u8>, suffix: &str) -> String {
            String::new()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct MixedArgs < 'a >"));
    assert!(contains_pattern(&client, "name : & 'a str"));
    assert!(contains_pattern(&client, "count : u32"));
    assert!(contains_pattern(&client, "data : Vec < u8 >"));
    assert!(contains_pattern(&client, "suffix : & 'a str"));
}

#[test]
fn test_cow_str() {
    let input: ItemFn = parse_quote! {
        pub fn takes_cow(s: std::borrow::Cow<'_, str>) -> String {
            s.into_owned()
        }
    };
    let client = generate_client(&input);
    // Cow has a lifetime, should be detected
    assert!(contains_pattern(&client, "struct TakesCowArgs"));
}

#[test]
fn test_box_of_ref() {
    let input: ItemFn = parse_quote! {
        pub fn takes_boxed_ref(b: Box<&str>) -> String {
            b.to_string()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "struct TakesBoxedRefArgs < 'a >"));
    assert!(contains_pattern(&client, "Box < & 'a str >"));
}

#[test]
fn test_ref_path_buf() {
    let input: ItemFn = parse_quote! {
        pub fn takes_path(p: &std::path::Path) -> String {
            p.display().to_string()
        }
    };
    let client = generate_client(&input);
    assert!(contains_pattern(&client, "& 'a std :: path :: Path"));
}

#[test]
fn test_has_reference_nested_option_vec() {
    let ty: Type = parse_quote!(Option<Vec<&str>>);
    assert!(has_reference_type(&ty));
}

#[test]
fn test_has_reference_result_with_ref() {
    let ty: Type = parse_quote!(Result<&str, String>);
    assert!(has_reference_type(&ty));
}

#[test]
fn test_has_reference_deeply_nested() {
    let ty: Type = parse_quote!(Option<Result<Vec<&str>, String>>);
    assert!(has_reference_type(&ty));
}

#[test]
fn test_has_reference_no_ref_in_generics() {
    let ty: Type = parse_quote!(Option<Vec<String>>);
    assert!(!has_reference_type(&ty));
}

#[test]
fn test_transform_nested_option_ref() {
    let ty: Type = parse_quote!(Option<&str>);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("Option < & 'a str >"));
}

#[test]
fn test_transform_result_with_refs() {
    let ty: Type = parse_quote!(Result<&str, &str>);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    let normalized = normalize_tokens(&transformed);
    assert!(normalized.contains("Result < & 'a str , & 'a str >"));
}

#[test]
fn test_transform_vec_of_ref() {
    let ty: Type = parse_quote!(Vec<&str>);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("Vec < & 'a str >"));
}

#[test]
fn test_transform_tuple_refs() {
    let ty: Type = parse_quote!((&str, i32, &str));
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    let normalized = normalize_tokens(&transformed);
    assert!(normalized.contains("(& 'a str , i32 , & 'a str)"));
}

#[test]
fn test_transform_array_of_refs() {
    let ty: Type = parse_quote!([&str; 2]);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("[& 'a str ; 2]"));
}

#[test]
fn test_transform_mut_ref() {
    let ty: Type = parse_quote!(&mut str);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("& 'a mut str"));
}

#[test]
fn test_transform_double_ref() {
    let ty: Type = parse_quote!(&&str);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("& 'a & 'a str"));
}

#[test]
fn test_transform_preserves_static_in_nested() {
    let ty: Type = parse_quote!(Option<&'static str>);
    let transformed = transform_ref_to_lifetime(&ty, Span::call_site());
    assert!(normalize_tokens(&transformed).contains("'static"));
}
