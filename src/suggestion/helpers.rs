//! Utility functions for SQL autocomplete suggestion.

use crate::sql::{token::Token, token_kind::TokenKind};
use crate::{Column, DataType, Database, Suggestion, Suggestions};

/// Skip past a parenthesized expression starting at `start` (one past the opening paren).
/// Returns the index one past the closing paren.
pub(super) fn skip_parens(tokens: &[Token], start: usize) -> usize {
    let mut depth = 1;
    for (i, t) in tokens.iter().enumerate().skip(start) {
        match &t.kind {
            TokenKind::ParenOpen => depth += 1,
            TokenKind::ParenClose => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return i + 1;
        }
    }
    tokens.len()
}

/// Parse column definitions from a parenthesized list (e.g., `(col1, col2, col3)`).
/// Returns the columns and the index after the closing paren.
pub(super) fn parse_column_defs(tokens: &[Token], start: usize) -> (Vec<Column>, usize) {
    if !tokens
        .get(start)
        .is_some_and(|t| matches!(t.kind, TokenKind::ParenOpen))
    {
        return (vec![], start);
    }
    let mut cols = Vec::new();
    let mut i = start + 1;
    while let Some(t) = tokens.get(i) {
        match &t.kind {
            TokenKind::ParenClose => {
                i += 1;
                break;
            }
            TokenKind::Comma => i += 1,
            _ => {
                if let Some(col) = t.ident() {
                    cols.push(Column::new(col, DataType::Unknown));
                }
                i += 1;
            }
        }
    }
    (cols, i)
}

/// Extract a qualified prefix (table/alias name before a dot) from the SQL region.
pub(super) fn qualified_prefix(sql: &str, select_end: usize, cursor_pos: usize) -> Option<String> {
    if cursor_pos <= select_end {
        return None;
    }
    let region = &sql[select_end..cursor_pos];
    region.rfind('.').and_then(|dot| {
        let before = region[..dot].trim_end();
        if let Some(stripped) = before.strip_suffix('"')
            && let Some(start) = stripped.rfind('"')
        {
            let ident = &stripped[start + 1..];
            return (!ident.is_empty()).then(|| ident.to_string());
        }
        let ident = before
            .rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .next()
            .unwrap_or("");
        (!ident.is_empty()).then(|| ident.to_string())
    })
}

/// Extract a qualified prefix from tokens based on position.
pub(super) fn qualified_prefix_from_pos(
    tokens: &[Token],
    start_pos: usize,
    cursor_pos: usize,
) -> Option<String> {
    tokens
        .iter()
        .enumerate()
        .take_while(|(_, t)| t.start < cursor_pos)
        .find_map(|(i, t)| {
            (matches!(t.kind, TokenKind::Dot) && t.start >= start_pos && i > 0)
                .then(|| tokens[i - 1].ident().map(|s| s.to_string()))
                .flatten()
        })
}

/// Gather columns from a table in the database metadata.
pub(super) fn gather_columns(meta: &Database, table: &str, out: &mut Suggestions) {
    for schema in meta.schemas.values() {
        if let Some(t) = schema.tables.get(table) {
            out.extend(t.columns.values().cloned().map(Suggestion::Column));
        }
    }
}
