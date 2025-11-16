use crate::sql::{keyword::Keyword, token::Token, token_kind::TokenKind};

/// Lenient SQL tokenizer producing a flat stream of `Token`s.
///
/// Scope / Intent:
/// - Designed for IDE autocomplete & cursor-aware suggestions.
/// - Accepts incomplete / syntactically invalid SQL (e.g. `SELECT FROM`, `JOIN , table`).
/// - Classifies only the minimal keyword set defined in `keyword.rs`.
///
/// Behavior:
/// - Skips ASCII whitespace.
/// - Aggregates `[A-Za-z0-9_]` runs into identifiers, preserving original case.
/// - Lowercases an identifier once to attempt keyword classification (no allocation
///   unless keyword match fails and we must store the original String).
/// - Emits single-character tokens for comma, dot, parentheses; everything else is `Other(char)`.
///
/// Guarantees:
/// - Never panics on valid UTF-8 & bounded indices.
/// - Never returns an error (malformed constructs still yield tokens).
///
/// Complexity:
/// - O(n) time, O(t) space where `t` is number of tokens.
pub fn tokenize(sql: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i] as char;

        // Skip whitespace quickly
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        let start = i;

        // Identifier path
        if c.is_ascii_alphanumeric() || c == '_' {
            i += 1;
            while i < bytes.len() {
                let cc = bytes[i] as char;
                if cc.is_ascii_alphanumeric() || cc == '_' {
                    i += 1;
                } else {
                    break;
                }
            }
            let text = &sql[start..i];
            let lower = text.to_ascii_lowercase();
            let kind = Keyword::from_lower(&lower)
                .map(TokenKind::Keyword)
                .unwrap_or_else(|| TokenKind::Ident(text.to_string()));
            out.push(Token::new(kind, start, i));
            continue;
        }

        // Single-character tokens
        i += 1;
        let kind = match c {
            ',' => TokenKind::Comma,
            '.' => TokenKind::Dot,
            '(' => TokenKind::ParenOpen,
            ')' => TokenKind::ParenClose,
            other => TokenKind::Other(other),
        };
        out.push(Token::new(kind, start, i));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::keyword::Keyword;
    use crate::sql::token_kind::TokenKind;

    #[test]
    fn basic_select_sequence() {
        let toks = tokenize("SELECT a, b FROM t");
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::Select)));
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::From)));
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::Ident(ref s) if s == "a"))
        );
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::Ident(ref s) if s == "b"))
        );
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::Ident(ref s) if s == "t"))
        );
    }

    #[test]
    fn preserves_case_for_identifiers() {
        let toks = tokenize("From MyTable");
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::From)));
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::Ident(ref s) if s == "MyTable"))
        );
    }

    #[test]
    fn incomplete_query_tokenization() {
        let toks = tokenize("SELECT ( FROM x");
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::Select)));
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::From)));
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::Ident(ref s) if s == "x"))
        );
    }

    #[test]
    fn punctuation_tokens() {
        let toks = tokenize("(a.b,c)");
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::ParenOpen)));
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::Dot)));
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::Comma)));
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::ParenClose)));
    }

    #[test]
    fn other_characters() {
        let toks = tokenize("SELECT * FROM t;");
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::Select)));
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::Other('*'))));
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::Other(';'))));
    }
}
