//! CTE and subquery column extraction.

use crate::sql::{keyword::Keyword, token::Token, token_kind::TokenKind};
use crate::{DataType, Database};

use super::helpers::skip_parens;
use super::types::DerivedTable;

/// Parse CTEs (WITH clauses) and extract their projected columns.
pub(super) async fn parse_ctes(tokens: &[Token], meta: &Database) -> Vec<DerivedTable> {
    let mut ctes = Vec::new();
    if !tokens.first().is_some_and(|t| t.is_keyword(Keyword::With)) {
        return ctes;
    }
    let mut i = 1;
    if tokens
        .get(i)
        .is_some_and(|t| t.is_keyword(Keyword::Recursive))
    {
        i += 1;
    }

    while i < tokens.len() {
        let Some(cte_name) = tokens.get(i).and_then(|t| t.ident()) else {
            break;
        };
        i += 1;
        if !tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::As)) {
            break;
        }
        i += 1;
        if !tokens
            .get(i)
            .is_some_and(|t| matches!(t.kind, TokenKind::ParenOpen))
        {
            break;
        }
        let paren_start = i;
        i += 1;

        let mut depth = 1;
        let mut subquery_end = i;
        while i < tokens.len() && depth > 0 {
            match &tokens[i].kind {
                TokenKind::ParenOpen => depth += 1,
                TokenKind::ParenClose => {
                    depth -= 1;
                    if depth == 0 {
                        subquery_end = i;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let columns = extract_subquery_columns(&tokens[paren_start + 1..subquery_end], meta).await;
        ctes.push(DerivedTable {
            alias: cte_name.to_string(),
            columns,
        });

        if tokens
            .get(i)
            .is_some_and(|t| matches!(t.kind, TokenKind::Comma))
        {
            i += 1;
            continue;
        }
        break;
    }
    ctes
}

/// Extract columns from a subquery's SELECT projection.
pub(super) async fn extract_subquery_columns(
    tokens: &[Token],
    _meta: &Database,
) -> Vec<(String, DataType)> {
    let mut columns = Vec::new();
    let Some(select_idx) = tokens.iter().position(|t| t.is_keyword(Keyword::Select)) else {
        return columns;
    };

    let mut depth = 0;
    let from_idx = tokens
        .iter()
        .enumerate()
        .skip(select_idx + 1)
        .find_map(|(idx, t)| {
            match &t.kind {
                TokenKind::ParenOpen => depth += 1,
                TokenKind::ParenClose => depth -= 1,
                _ => {}
            }
            (depth == 0 && t.is_keyword(Keyword::From)).then_some(idx)
        });
    let projection_end = from_idx.unwrap_or(tokens.len());
    let mut i = select_idx + 1;

    if tokens.get(i).is_some_and(|t| t.ident() == Some("DISTINCT")) {
        i += 1;
        if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::On))
            && tokens
                .get(i + 1)
                .is_some_and(|t| matches!(t.kind, TokenKind::ParenOpen))
        {
            i = skip_parens(tokens, i + 2);
        }
    }

    while i < projection_end {
        let t = &tokens[i];

        if matches!(t.kind, TokenKind::ParenOpen) {
            i = skip_parens(tokens, i + 1);
            if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::As)) {
                i += 1;
                if let Some(alias) = tokens.get(i).and_then(|t| t.ident()) {
                    columns.push((alias.to_string(), DataType::Unknown));
                    i += 1;
                }
            }
            while i < projection_end
                && !matches!(tokens.get(i).map(|t| &t.kind), Some(TokenKind::Comma))
            {
                i += 1;
            }
            if i < projection_end {
                i += 1;
            }
            continue;
        }

        if matches!(t.kind, TokenKind::Other('*')) {
            i += 1;
            if i < projection_end
                && matches!(tokens.get(i).map(|t| &t.kind), Some(TokenKind::Comma))
            {
                i += 1;
            }
            continue;
        }

        if t.ident().is_some()
            && tokens
                .get(i + 1)
                .is_some_and(|t| matches!(t.kind, TokenKind::Dot))
            && tokens
                .get(i + 2)
                .is_some_and(|t| matches!(t.kind, TokenKind::Other('*')))
        {
            i += 3;
            if i < projection_end
                && matches!(tokens.get(i).map(|t| &t.kind), Some(TokenKind::Comma))
            {
                i += 1;
            }
            continue;
        }

        let expr_start = i;
        let mut paren_depth = 0;
        while i < projection_end {
            match &tokens[i].kind {
                TokenKind::ParenOpen => paren_depth += 1,
                TokenKind::ParenClose => paren_depth -= 1,
                TokenKind::Comma if paren_depth == 0 => break,
                _ => {}
            }
            i += 1;
        }
        let expr_end = i;

        let col_name = (expr_start..expr_end)
            .rev()
            .find_map(|j| {
                if tokens.get(j).is_some_and(|t| t.is_keyword(Keyword::As)) {
                    tokens
                        .get(j + 1)
                        .and_then(|t| t.ident())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
            .or_else(|| {
                tokens
                    .get(expr_end - 1)
                    .and_then(|t| t.ident())
                    .and_then(|name| {
                        if expr_end - 1 == expr_start
                            || tokens
                                .get(expr_end - 2)
                                .is_some_and(|t| matches!(t.kind, TokenKind::Dot))
                            || !matches!(
                                tokens.get(expr_end - 2).map(|t| &t.kind),
                                Some(TokenKind::ParenClose)
                            )
                        {
                            Some(name.to_string())
                        } else {
                            None
                        }
                    })
            });

        if let Some(name) = col_name {
            columns.push((name, DataType::Unknown));
        }
        if i < projection_end && matches!(tokens.get(i).map(|t| &t.kind), Some(TokenKind::Comma)) {
            i += 1;
        }
    }
    columns
}
