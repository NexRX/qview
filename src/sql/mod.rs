//! Lightweight, lenient SQL tokenization / micro‑AST module.
//!
//! This module groups the minimal building blocks used by the autocomplete
//! engine to reason about a SQL query near a cursor position without requiring
//! a full parser. The components are intentionally pragmatic:
//!
//! Modules:
//! - `keyword`    : Small enum of only the keywords needed for suggestions.
//! - `token_kind` : Classification of lexical atoms (identifiers, punctuation, keywords).
//! - `token`      : Token struct pairing a `TokenKind` with source span offsets.
//! - `tokenizer`  : Single pass O(n) tokenizer producing a `Vec<Token>` from raw SQL.
//!
//! Design Principles:
//! 1. Accept incomplete / syntactically invalid SQL (robust for live editing).
//! 2. Preserve original identifier casing for display & lookup.
//! 3. Keep keyword set purposely small; extend only when completion logic demands.
//! 4. Avoid allocations except when creating identifier `String`s.
//!
//! Public Re‑exports:
//! You can `use crate::sql::{tokenize, Token, TokenKind, Keyword};` directly,
//! or pull everything via the `prelude` submodule.
//!
//! Example:
//! ```rust
//! use qview::sql::prelude::*;
//!
//! let tokens = tokenize("SELECT a, b FROM my_table");
//! assert!(tokens.iter().any(|t| t.is_keyword(Keyword::Select)));
//! assert!(tokens.iter().any(|t| t.ident() == Some("my_table")));
//! ```
//!
//! Extensibility:
//! If you add new completion contexts (e.g. support for CTEs or window
//! functions), prefer adding *only* the required keywords / punctuation rather
//! than attempting full SQL coverage.
//!
//! NOTE: This is **not** a full SQL parser and intentionally ignores many
//! constructs that are not needed for current autocomplete heuristics.

pub mod keyword;
pub mod token;
pub mod token_kind;
pub mod tokenizer;

pub use keyword::Keyword;
pub use token::Token;
pub use token_kind::TokenKind;
pub use tokenizer::tokenize;

/// Convenience prelude re‑exporting the most commonly used items.
///
/// Import with:
/// `use qview::sql::prelude::*;`
pub mod prelude {
    pub use super::{Keyword, Token, TokenKind, tokenize};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_and_access() {
        let sql = "SELECT col FROM tbl";
        let tokens = tokenize(sql);
        assert!(tokens.iter().any(|t| t.is_keyword(Keyword::Select)));
        assert!(tokens.iter().any(|t| t.is_keyword(Keyword::From)));
        assert!(tokens.iter().any(|t| t.ident() == Some("col")));
        assert!(tokens.iter().any(|t| t.ident() == Some("tbl")));
    }

    #[test]
    fn prelude_import_works() {
        use super::prelude::*;
        let toks = tokenize("FROM X");
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::From)));
        assert!(toks.iter().any(|t| t.ident() == Some("X")));
    }
}
