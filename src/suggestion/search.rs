//! Search implementations for SELECT and DML statements.

use crate::sql::token::Token;
use crate::{Database, Result, Suggestion, Suggestions};

use super::cte::parse_ctes;
use super::from_clause::{extract_tables, locate_from};
use super::helpers::{gather_columns, qualified_prefix, qualified_prefix_from_pos};
use super::types::{DerivedTable, StatementContext, StatementType};

/// Search for column suggestions in a SELECT statement.
pub(super) async fn search_select(
    sql: &str,
    tokens: &[Token],
    cursor_pos: usize,
    meta: &Database,
    ctes: &[DerivedTable],
    select_idx: usize,
    select_depth: i32,
) -> Result<Suggestions> {
    let Some(from_idx) = locate_from(tokens, select_idx, select_depth) else {
        return Ok(vec![]);
    };
    let (tables, aliases, derived_tables) =
        extract_tables(tokens, from_idx, select_depth, meta, ctes).await;

    if let Some(prefix) = qualified_prefix(sql, tokens[select_idx].end, cursor_pos) {
        let mut out = Vec::new();
        if let Some(dt) = derived_tables.iter().find(|dt| dt.alias == prefix) {
            out.extend(
                dt.columns
                    .iter()
                    .map(|(c, d)| Suggestion::Column(c.clone(), d.clone())),
            );
        } else if let Some(cte) = ctes.iter().find(|c| c.alias == prefix) {
            out.extend(
                cte.columns
                    .iter()
                    .map(|(c, d)| Suggestion::Column(c.clone(), d.clone())),
            );
        } else {
            gather_columns(meta, aliases.get(&prefix).unwrap_or(&prefix), &mut out).await;
        }
        return Ok(out);
    }

    let mut out = Vec::new();
    for dt in &derived_tables {
        out.extend(
            dt.columns
                .iter()
                .map(|(c, d)| Suggestion::Column(c.clone(), d.clone())),
        );
    }
    for cte in ctes.iter().filter(|c| tables.contains(&c.alias)) {
        out.extend(
            cte.columns
                .iter()
                .map(|(c, d)| Suggestion::Column(c.clone(), d.clone())),
        );
    }
    for tbl in tables
        .iter()
        .filter(|t| !ctes.iter().any(|c| c.alias == **t))
    {
        gather_columns(meta, tbl, &mut out).await;
    }
    Ok(out)
}

/// Search for column suggestions in DML statements (INSERT, UPDATE, DELETE).
pub(super) async fn search_dml(
    tokens: &[Token],
    cursor_pos: usize,
    meta: &Database,
    ctx: &StatementContext,
) -> Result<Suggestions> {
    let Some(target) = &ctx.target_table else {
        return Ok(vec![]);
    };
    let should_suggest = match ctx.stmt_type {
        StatementType::Insert => ctx.in_returning || ctx.in_column_list,
        StatementType::Update => ctx.in_set || ctx.in_returning,
        StatementType::Delete => ctx.in_returning,
        _ => false,
    };
    if !should_suggest {
        return Ok(vec![]);
    }

    let mut out = Vec::new();
    if qualified_prefix_from_pos(tokens, ctx.clause_start, cursor_pos).is_none_or(|p| p == *target)
    {
        gather_columns(meta, target, &mut out).await;
    }
    Ok(out)
}

/// Main entry point for searching suggestions.
pub(super) async fn search(
    sql: &str,
    tokens: &[Token],
    cursor_pos: usize,
    meta: &Database,
) -> Result<Suggestions> {
    let ctes = parse_ctes(tokens, meta).await;
    let Some(ctx) = super::context::detect_statement_context(tokens, cursor_pos) else {
        return Ok(vec![]);
    };

    match ctx.stmt_type {
        StatementType::Select => {
            search_select(
                sql,
                tokens,
                cursor_pos,
                meta,
                &ctes,
                ctx.stmt_idx,
                ctx.depth,
            )
            .await
        }
        _ => search_dml(tokens, cursor_pos, meta, &ctx).await,
    }
}
