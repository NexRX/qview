//! FROM clause parsing and table extraction.

use std::collections::HashMap;

use crate::Database;
use crate::sql::{keyword::Keyword, token::Token, token_kind::TokenKind};

use super::cte::extract_subquery_columns;
use super::helpers::{parse_column_defs, skip_parens};
use super::types::DerivedTable;

/// Locate the FROM keyword for a SELECT statement.
pub(super) fn locate_from(tokens: &[Token], select_idx: usize, select_depth: i32) -> Option<usize> {
    let mut depth = select_depth;
    tokens
        .iter()
        .enumerate()
        .skip(select_idx + 1)
        .find_map(|(idx, t)| {
            match t.kind {
                TokenKind::ParenOpen => depth += 1,
                TokenKind::ParenClose => depth -= 1,
                _ => {}
            }
            (depth == select_depth && t.is_keyword(Keyword::From)).then_some(idx)
        })
}

/// Try to extract an alias for a table.
fn try_extract_alias(
    tokens: &[Token],
    i: usize,
    table: &str,
    aliases: &mut HashMap<String, String>,
) -> usize {
    if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::As)) {
        if let Some(a) = tokens.get(i + 1).and_then(|t| t.ident()) {
            aliases.insert(a.to_string(), table.to_string());
            return 2;
        }
    } else if let Some(a) = tokens.get(i).and_then(|t| {
        if !matches!(t.kind, TokenKind::Keyword(_)) {
            t.ident()
        } else {
            None
        }
    }) {
        aliases.insert(a.to_string(), table.to_string());
        return 1;
    }
    0
}

/// Extract table names, aliases, and derived tables from the FROM clause.
pub(super) async fn extract_tables(
    tokens: &[Token],
    from_idx: usize,
    select_depth: i32,
    meta: &Database,
    _ctes: &[DerivedTable],
) -> (Vec<String>, HashMap<String, String>, Vec<DerivedTable>) {
    let mut tables = Vec::new();
    let mut aliases = HashMap::new();
    let mut derived_tables = Vec::new();
    let mut depth = select_depth;
    let mut i = from_idx + 1;

    while let Some(t) = tokens.get(i) {
        // Handle parenthesis tracking
        match t.kind {
            TokenKind::ParenOpen => {
                // Check if this is a derived table (subquery)
                // Look ahead to see if there's a SELECT
                if tokens
                    .get(i + 1)
                    .is_some_and(|t| t.is_keyword(Keyword::Select))
                {
                    // This is a derived table
                    let subquery_start = i + 1;
                    depth += 1;
                    i += 1;

                    // Find the matching closing paren
                    i = skip_parens(tokens, i);
                    let subquery_end = i - 1;
                    depth -= 1;

                    // Extract columns from subquery
                    let columns =
                        extract_subquery_columns(&tokens[subquery_start..subquery_end], meta).await;

                    // Look for alias after the closing paren
                    // Skip AS if present
                    if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::As)) {
                        i += 1;
                    }

                    if let Some(alias) = tokens.get(i).and_then(|t| t.ident()) {
                        derived_tables.push(DerivedTable {
                            alias: alias.to_string(),
                            columns,
                        });
                        i += 1;
                    }
                    continue;
                }

                // Check if this is a parenthesized join group: (a JOIN b ON ...) alias
                // We need to recursively extract tables from inside
                depth += 1;
                i += 1;
                continue;
            }
            TokenKind::ParenClose => {
                depth -= 1;
                if depth < select_depth {
                    break;
                }
                i += 1;
                continue;
            }
            _ => {}
        }

        // Only process tokens at our target SELECT depth
        if depth != select_depth {
            i += 1;
            continue;
        }

        if let TokenKind::Keyword(k) = &t.kind {
            match k {
                _ if Keyword::TERMINATORS.contains(k) => break,
                _ if Keyword::JOIN_MODIFIERS.contains(k) => {
                    i += 1;
                    continue;
                }
                Keyword::Join | Keyword::Only | Keyword::Lateral => {
                    i += 1;
                    continue;
                }
                Keyword::On => {
                    i += 1;
                    while let Some(t) = tokens.get(i) {
                        match &t.kind {
                            TokenKind::ParenOpen => i = skip_parens(tokens, i + 1),
                            TokenKind::Comma => break,
                            TokenKind::Keyword(k)
                                if *k == Keyword::Join
                                    || Keyword::JOIN_MODIFIERS.contains(k)
                                    || Keyword::TERMINATORS.contains(k) =>
                            {
                                break;
                            }
                            _ => i += 1,
                        }
                    }
                    continue;
                }
                Keyword::Using => {
                    i += 1;
                    if tokens
                        .get(i)
                        .is_some_and(|t| matches!(t.kind, TokenKind::ParenOpen))
                    {
                        i = skip_parens(tokens, i + 1);
                    }
                    continue;
                }
                _ => {}
            }
        }

        if t.is_keyword(Keyword::Values) {
            i += 1;
            while tokens
                .get(i)
                .is_some_and(|t| matches!(t.kind, TokenKind::ParenOpen | TokenKind::Comma))
            {
                if matches!(tokens[i].kind, TokenKind::ParenOpen) {
                    i = skip_parens(tokens, i + 1);
                } else {
                    i += 1;
                }
            }
            if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::As)) {
                i += 1;
            }
            if let Some(alias) = tokens.get(i).and_then(|t| t.ident()) {
                i += 1;
                let (cols, new_i) = parse_column_defs(tokens, i);
                i = new_i;
                derived_tables.push(DerivedTable {
                    alias: alias.to_string(),
                    columns: cols,
                });
            }
            continue;
        }

        if let Some(name) = t.ident() {
            let name = name.to_string();
            // schema.table pattern
            if tokens
                .get(i + 1)
                .is_some_and(|t| matches!(t.kind, TokenKind::Dot))
                && let Some(tbl) = tokens.get(i + 2).and_then(|t| t.ident())
            {
                let tbl = tbl.to_string();
                if !tables.contains(&tbl) {
                    tables.push(tbl.clone());
                }
                i += 3;
                i += try_extract_alias(tokens, i, &tbl, &mut aliases);
                continue;
            }
            // function call
            if tokens
                .get(i + 1)
                .is_some_and(|t| matches!(t.kind, TokenKind::ParenOpen))
            {
                i = skip_parens(tokens, i + 2);
                if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::As)) {
                    i += 1;
                }
                if let Some(alias) = tokens.get(i).and_then(|t| t.ident()) {
                    i += 1;
                    let (cols, new_i) = parse_column_defs(tokens, i);
                    i = new_i;
                    derived_tables.push(DerivedTable {
                        alias: alias.to_string(),
                        columns: cols,
                    });
                }
                continue;
            }
            // regular table
            if !tables.contains(&name) {
                tables.push(name.clone());
            }
            i += 1;
            i += try_extract_alias(tokens, i, &name, &mut aliases);
            continue;
        }

        i += 1;
    }

    (tables, aliases, derived_tables)
}
