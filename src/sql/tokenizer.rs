use crate::sql::{keyword::Keyword, token::Token, token_kind::TokenKind};

/// Represents a quoted identifier with its content (without the quotes).
fn parse_quoted_identifier(sql: &str, start: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut i = start + 1;
    let mut content = String::new();
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '"' {
            // Check for escaped quote ""
            if bytes.get(i + 1) == Some(&b'"') {
                content.push('"');
                i += 2;
                continue;
            }
            // End of quoted identifier
            return Some((content, i + 1));
        }
        content.push(c);
        i += 1;
    }
    // Unclosed quote - return what we have
    Some((content, i))
}

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

        // Quoted identifier path ("identifier")
        if c == '"'
            && let Some((content, end)) = parse_quoted_identifier(sql, i)
        {
            out.push(Token::new(TokenKind::QuotedIdent(content), start, end));
            i = end;
            continue;
        }

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

        // Multi-character operators (JSON, etc.)
        if c == '-' && bytes.get(i + 1) == Some(&b'>') {
            if bytes.get(i + 2) == Some(&b'>') {
                // ->> operator
                out.push(Token::new(TokenKind::JsonTextOp, start, i + 3));
                i += 3;
                continue;
            }
            // -> operator
            out.push(Token::new(TokenKind::JsonOp, start, i + 2));
            i += 2;
            continue;
        }

        // Single-character tokens
        i += 1;
        let kind = match c {
            ',' => TokenKind::Comma,
            '.' => TokenKind::Dot,
            '(' => TokenKind::ParenOpen,
            ')' => TokenKind::ParenClose,
            '[' => TokenKind::BracketOpen,
            ']' => TokenKind::BracketClose,
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
    fn quoted_identifiers() {
        let toks = tokenize(r#"SELECT "User Name" FROM "My Table""#);
        assert!(toks.iter().any(|t| t.is_keyword(Keyword::Select)));
        assert!(
            toks.iter()
                .any(|t| matches!(&t.kind, TokenKind::QuotedIdent(s) if s == "User Name"))
        );
        assert!(
            toks.iter()
                .any(|t| matches!(&t.kind, TokenKind::QuotedIdent(s) if s == "My Table"))
        );
    }

    #[test]
    fn quoted_identifier_with_escaped_quote() {
        let toks = tokenize(r#"SELECT "He said ""hello""" FROM t"#);
        assert!(
            toks.iter()
                .any(|t| matches!(&t.kind, TokenKind::QuotedIdent(s) if s == r#"He said "hello""#))
        );
    }

    #[test]
    fn bracket_tokens() {
        let toks = tokenize("SELECT tags[1] FROM t");
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::BracketOpen))
        );
        assert!(
            toks.iter()
                .any(|t| matches!(t.kind, TokenKind::BracketClose))
        );
    }

    #[test]
    fn json_operators() {
        let toks = tokenize("SELECT data->'key', data->>'value' FROM t");
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::JsonOp)));
        assert!(toks.iter().any(|t| matches!(t.kind, TokenKind::JsonTextOp)));
    }

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
