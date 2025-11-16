//! Token model tying a `TokenKind` to its source span.
//!
//! This module is part of the lightweight / lenient SQL tokenizer used for
//! autocomplete. A `Token` is intentionally minimal: its classification (`kind`)
//! plus byte offsets (`start`, `end`) into the original SQL string.
//!
//! Rationale:
//! - Offsets let higher‑level logic (e.g. cursor aware completion) slice the
//!   original query without needing a parallel reconstructed string.
//! - Keeping `Token` immutable & simple reduces cognitive load; manipulating
//!   tokens should happen by constructing new ones, not mutating existing ones.
//!
//! See sibling modules:
//! - `keyword.rs`    for the `Keyword` enum.
//! - `token_kind.rs` for `TokenKind` classification.
//! - `tokenizer.rs`  for producing `Vec<Token>` from raw SQL input.
use crate::sql::{keyword::Keyword, token_kind::TokenKind};

/// A lexical token with its inclusive start and exclusive end byte offsets.
///
/// Offsets always refer to the *original* SQL string supplied to the tokenizer.
/// This allows downstream code to perform substring operations or cursor range
/// checks efficiently.
///
/// Invariants:
/// - `end >= start`
/// - `[start, end)` is a valid slice range for the original input
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
}

impl Token {
    /// Construct a new token.
    pub const fn new(kind: TokenKind, start: usize, end: usize) -> Self {
        Self { kind, start, end }
    }

    /// Byte length of this token (`end - start`).
    pub fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// True if the token's length is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the identifier text if this token is an identifier.
    pub fn ident(&self) -> Option<&str> {
        self.kind.ident()
    }

    /// Returns true if this token represents a given keyword.
    pub fn is_keyword(&self, kw: Keyword) -> bool {
        self.kind.is_keyword(kw)
    }

    /// Returns true if the cursor (byte offset) lies within this token's span.
    ///
    /// NOTE: End is exclusive, so `cursor == end` returns false.
    pub fn contains(&self, cursor: usize) -> bool {
        cursor >= self.start && cursor < self.end
    }

    /// True if the token starts before `cursor` and ends at or after it.
    /// Semantically equivalent to `contains(cursor)`.
    pub fn touches(&self, cursor: usize) -> bool {
        self.contains(cursor)
    }

    /// Convenience: convert to a `(start, end)` tuple.
    pub const fn span(&self) -> (usize, usize) {
        (self.start, self.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{keyword::Keyword, token_kind::TokenKind};

    #[test]
    fn length_and_empty() {
        let t = Token::new(TokenKind::Comma, 5, 6);
        assert_eq!(t.len(), 1);
        assert!(!t.is_empty());
    }

    #[test]
    fn ident_access() {
        let t = Token::new(TokenKind::Ident("Users".into()), 0, 5);
        assert_eq!(t.ident(), Some("Users"));
        assert!(t.contains(2));
        assert!(!t.contains(5)); // end exclusive
    }

    #[test]
    fn keyword_detection() {
        let t = Token::new(TokenKind::Keyword(Keyword::Select), 0, 6);
        assert!(t.is_keyword(Keyword::Select));
        assert!(!t.is_keyword(Keyword::From));
    }

    #[test]
    fn span_method() {
        let t = Token::new(TokenKind::Dot, 10, 11);
        assert_eq!(t.span(), (10, 11));
    }
}
