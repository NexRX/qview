//! Token kind definitions for the lightweight SQL tokenizer.
//!
//! Each `TokenKind` variant represents a syntactic atom discovered during the
//! lenient scanning phase. The tokenizer avoids strict SQL rules; anything
//! unrecognized becomes `Other(char)`.
//!
//! Design goals:
//! - Preserve original identifier casing via `Ident(String)` for downstream
//!   display and matching.
//! - Keep the set of structural punctuation minimal (comma, dot, parens) as
//!   that's sufficient for current completion heuristics.
//! - Provide ergonomic helpers (`is_keyword`, `ident`) to avoid verbose pattern
//!   matches at call sites.
//!
//! See `keyword.rs` for the `Keyword` enum and `tokenizer.rs` for tokenization.

use crate::sql::keyword::Keyword;

/// Classification for a token produced by the tokenizer.
///
/// Not a full SQL lexeme set; intentionally small and pragmatic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    /// Table / alias / column / generic identifier.
    Ident(String),
    /// Quoted identifier (e.g., `"User Name"`).
    QuotedIdent(String),
    /// Recognized SQL keyword.
    Keyword(Keyword),
    /// Comma `,` (used to separate table items in FROM, list items, etc.).
    Comma,
    /// Dot `.` (used for qualified names like `table.column`).
    Dot,
    /// Opening parenthesis `(`.
    ParenOpen,
    /// Closing parenthesis `)`.
    ParenClose,
    /// Opening bracket `[` (used for array subscripts).
    BracketOpen,
    /// Closing bracket `]`.
    BracketClose,
    /// JSON arrow operator `->`.
    JsonOp,
    /// JSON text arrow operator `->>`.
    JsonTextOp,
    /// Any other single punctuation / symbol we do not specially classify.
    Other(char),
}

impl TokenKind {
    /// True if this token is the given keyword.
    pub fn is_keyword(&self, kw: Keyword) -> bool {
        matches!(self, TokenKind::Keyword(k) if *k == kw)
    }

    /// Returns the identifier text if this token is an `Ident` or `QuotedIdent`.
    pub fn ident(&self) -> Option<&str> {
        match self {
            TokenKind::Ident(s) | TokenKind::QuotedIdent(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Convenience: returns true if this token represents any identifier (quoted or unquoted).
    pub fn is_ident(&self) -> bool {
        matches!(self, TokenKind::Ident(_) | TokenKind::QuotedIdent(_))
    }

    /// Returns true if this token is structural punctuation (non-ident, non-keyword).
    pub fn is_punctuation(&self) -> bool {
        matches!(
            self,
            TokenKind::Comma
                | TokenKind::Dot
                | TokenKind::ParenOpen
                | TokenKind::ParenClose
                | TokenKind::BracketOpen
                | TokenKind::BracketClose
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::keyword::Keyword;

    #[test]
    fn keyword_detection() {
        let tk = TokenKind::Keyword(Keyword::Select);
        assert!(tk.is_keyword(Keyword::Select));
        assert!(!tk.is_keyword(Keyword::From));
        assert!(tk.ident().is_none());
    }

    #[test]
    fn ident_access() {
        let tk = TokenKind::Ident("MyTable".into());
        assert!(tk.is_ident());
        assert_eq!(tk.ident(), Some("MyTable"));
        assert!(!tk.is_punctuation());
    }

    #[test]
    fn punctuation_classification() {
        assert!(TokenKind::Comma.is_punctuation());
        assert!(TokenKind::Dot.is_punctuation());
        assert!(TokenKind::ParenOpen.is_punctuation());
        assert!(TokenKind::ParenClose.is_punctuation());
        assert!(TokenKind::BracketOpen.is_punctuation());
        assert!(TokenKind::BracketClose.is_punctuation());
        assert!(!TokenKind::Ident("x".into()).is_punctuation());
        assert!(!TokenKind::QuotedIdent("x".into()).is_punctuation());
        assert!(!TokenKind::Keyword(Keyword::From).is_punctuation());
    }

    #[test]
    fn quoted_ident_access() {
        let tk = TokenKind::QuotedIdent("My Table".into());
        assert!(tk.is_ident());
        assert_eq!(tk.ident(), Some("My Table"));
        assert!(!tk.is_punctuation());
    }

    #[test]
    fn json_operators_not_punctuation() {
        assert!(!TokenKind::JsonOp.is_punctuation());
        assert!(!TokenKind::JsonTextOp.is_punctuation());
    }

    #[test]
    fn other_variant() {
        let tk = TokenKind::Other(';');
        assert!(!tk.is_ident());
        assert!(!tk.is_punctuation());
        assert!(tk.ident().is_none());
    }
}
