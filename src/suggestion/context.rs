//! Statement context detection and DML context building.

use crate::sql::{keyword::Keyword, token::Token, token_kind::TokenKind};

use super::types::{StatementContext, StatementPositions, StatementType, StmtPos};

/// Detect the statement context at the cursor position.
pub(super) fn detect_statement_context(
    tokens: &[Token],
    cursor_pos: usize,
) -> Option<StatementContext> {
    let (last_select, last_insert, last_update, last_delete, returning_depth) =
        scan_statement_positions(tokens, cursor_pos);

    let build_dml = |t: StatementType, i: usize, d: i32| match t {
        StatementType::Insert => build_insert_context(tokens, i, d, cursor_pos),
        StatementType::Update => build_update_context(tokens, i, d, cursor_pos),
        StatementType::Delete => build_delete_context(tokens, i, d, cursor_pos),
        _ => unreachable!(),
    };

    let dml_stmts = [
        (StatementType::Insert, last_insert),
        (StatementType::Update, last_update),
        (StatementType::Delete, last_delete),
    ];

    // If in RETURNING clause, prefer DML at that depth
    if let Some(ret_d) = returning_depth
        && let Some((t, i, d)) = dml_stmts
            .iter()
            .filter_map(|(t, opt)| opt.filter(|(_, d)| *d == ret_d).map(|(i, d)| (*t, i, d)))
            .max_by_key(|(_, i, _)| *i)
    {
        return build_dml(t, i, d);
    }

    // Prefer depth-0 DML over nested SELECTs
    let sel_nested = |idx: usize| last_select.is_none_or(|(i, d)| d > 0 || i < idx);
    for (t, opt) in &dml_stmts {
        if let Some((i, 0)) = opt
            && sel_nested(*i)
        {
            return build_dml(*t, *i, 0);
        }
    }

    // Fall back to most recent statement
    let (stmt_type, idx, depth) = [
        last_select.map(|(i, d)| (StatementType::Select, i, d)),
        last_insert.map(|(i, d)| (StatementType::Insert, i, d)),
        last_update.map(|(i, d)| (StatementType::Update, i, d)),
        last_delete.map(|(i, d)| (StatementType::Delete, i, d)),
    ]
    .into_iter()
    .flatten()
    .max_by_key(|(_, i, _)| *i)?;

    match stmt_type {
        StatementType::Select => Some(StatementContext {
            stmt_type,
            stmt_idx: idx,
            depth,
            target_table: None,
            in_returning: false,
            in_set: false,
            in_column_list: false,
            clause_start: tokens[idx].end,
        }),
        _ => build_dml(stmt_type, idx, depth),
    }
}

/// Scan tokens to find positions of statement keywords.
fn scan_statement_positions(tokens: &[Token], cursor_pos: usize) -> StatementPositions {
    let (mut depth, mut in_on_conflict) = (0, false);
    let (mut last_select, mut last_insert, mut last_update, mut last_delete, mut ret_depth): (
        StmtPos,
        StmtPos,
        StmtPos,
        StmtPos,
        Option<i32>,
    ) = (None, None, None, None, None);

    for (idx, t) in tokens
        .iter()
        .enumerate()
        .take_while(|(_, t)| t.start < cursor_pos)
    {
        match t.kind {
            TokenKind::ParenOpen => depth += 1,
            TokenKind::ParenClose => depth -= 1,
            _ => {}
        }
        match () {
            _ if t.is_keyword(Keyword::Select) => last_select = Some((idx, depth)),
            _ if t.is_keyword(Keyword::Insert) => {
                last_insert = Some((idx, depth));
                in_on_conflict = false;
            }
            _ if t.is_keyword(Keyword::On) && last_insert.is_some() => in_on_conflict = true,
            _ if t.is_keyword(Keyword::Update) && depth == 0 && !in_on_conflict => {
                last_update = Some((idx, depth))
            }
            _ if t.is_keyword(Keyword::Delete) => last_delete = Some((idx, depth)),
            _ if t.is_keyword(Keyword::Returning) => ret_depth = Some(depth),
            _ => {}
        }
    }
    (
        last_select,
        last_insert,
        last_update,
        last_delete,
        ret_depth,
    )
}

/// Parse a table name (possibly schema-qualified) from tokens.
fn parse_table_name(tokens: &[Token], start: usize) -> (Option<String>, usize) {
    let Some(name) = tokens.get(start).and_then(|t| t.ident()) else {
        return (None, 0);
    };
    if tokens
        .get(start + 1)
        .is_some_and(|t| matches!(t.kind, TokenKind::Dot))
        && let Some(table) = tokens.get(start + 2).and_then(|t| t.ident())
    {
        (Some(table.to_string()), 3)
    } else {
        (Some(name.to_string()), 1)
    }
}

/// Build context for INSERT statements.
fn build_insert_context(
    tokens: &[Token],
    stmt_idx: usize,
    depth: i32,
    cursor_pos: usize,
) -> Option<StatementContext> {
    let mut i = stmt_idx + 1;
    let default_start = tokens.get(stmt_idx).map_or(0, |t| t.end);
    if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::Into)) {
        i += 1;
    }
    let (target_table, consumed) = parse_table_name(tokens, i);
    i += consumed;

    let (col_start, col_end, val_start, ret_start) = scan_insert_clauses(tokens, i, cursor_pos);
    let (in_returning, in_column_list, clause_start) = match () {
        _ if ret_start.is_some_and(|s| cursor_pos >= s) => (true, false, ret_start.unwrap()),
        _ if val_start.is_some_and(|s| cursor_pos >= s) => (false, false, val_start.unwrap()),
        _ if col_start.is_some()
            && col_end.is_some()
            && cursor_pos >= col_start.unwrap()
            && cursor_pos <= col_end.unwrap() =>
        {
            (false, true, col_start.unwrap())
        }
        _ if col_start.is_some() && col_end.is_none() => (false, true, col_start.unwrap()),
        _ => (false, false, default_start),
    };

    Some(StatementContext {
        stmt_type: StatementType::Insert,
        stmt_idx,
        depth,
        target_table,
        in_returning,
        in_set: false,
        in_column_list,
        clause_start,
    })
}

/// Scan INSERT clauses (column list, VALUES, RETURNING).
fn scan_insert_clauses(
    tokens: &[Token],
    start: usize,
    cursor_pos: usize,
) -> (Option<usize>, Option<usize>, Option<usize>, Option<usize>) {
    let (mut paren_depth, mut col_start, mut col_end, mut val_start, mut ret_start) =
        (0, None, None, None, None);
    for t in tokens
        .iter()
        .skip(start)
        .take_while(|t| t.start < cursor_pos)
    {
        match &t.kind {
            TokenKind::ParenOpen => {
                if paren_depth == 0 && col_start.is_none() && val_start.is_none() {
                    col_start = Some(t.start);
                }
                paren_depth += 1;
            }
            TokenKind::ParenClose => {
                paren_depth -= 1;
                if paren_depth == 0 && col_end.is_none() && val_start.is_none() {
                    col_end = Some(t.end);
                }
            }
            TokenKind::Keyword(Keyword::Values) => val_start = Some(t.start),
            TokenKind::Keyword(Keyword::Returning) => ret_start = Some(t.end),
            _ => {}
        }
    }
    (col_start, col_end, val_start, ret_start)
}

/// Build context for UPDATE statements.
fn build_update_context(
    tokens: &[Token],
    stmt_idx: usize,
    depth: i32,
    cursor_pos: usize,
) -> Option<StatementContext> {
    let mut i = stmt_idx + 1;
    let default_start = tokens.get(stmt_idx).map_or(0, |t| t.end);
    if tokens.get(i).is_some_and(|t| t.is_keyword(Keyword::Only)) {
        i += 1;
    }
    let (target_table, consumed) = parse_table_name(tokens, i);
    i += consumed;

    let (set_start, where_start, ret_start) = scan_update_clauses(tokens, i, cursor_pos);
    let (in_returning, in_set, clause_start) = match () {
        _ if ret_start.is_some_and(|s| cursor_pos >= s) => (true, false, ret_start.unwrap()),
        _ if where_start.is_some_and(|s| cursor_pos >= s) => (false, false, where_start.unwrap()),
        _ if set_start.is_some_and(|s| cursor_pos >= s) => (false, true, set_start.unwrap()),
        _ => (false, false, default_start),
    };

    Some(StatementContext {
        stmt_type: StatementType::Update,
        stmt_idx,
        depth,
        target_table,
        in_returning,
        in_set,
        in_column_list: false,
        clause_start,
    })
}

/// Scan UPDATE clauses (SET, WHERE, RETURNING).
fn scan_update_clauses(
    tokens: &[Token],
    start: usize,
    cursor_pos: usize,
) -> (Option<usize>, Option<usize>, Option<usize>) {
    let (mut set, mut whr, mut ret) = (None, None, None);
    for t in tokens
        .iter()
        .skip(start)
        .take_while(|t| t.start < cursor_pos)
    {
        match &t.kind {
            TokenKind::Keyword(Keyword::Set) => set = Some(t.end),
            TokenKind::Keyword(Keyword::Where) => whr = Some(t.end),
            TokenKind::Keyword(Keyword::Returning) => ret = Some(t.end),
            _ => {}
        }
    }
    (set, whr, ret)
}

/// Build context for DELETE statements.
fn build_delete_context(
    tokens: &[Token],
    stmt_idx: usize,
    depth: i32,
    cursor_pos: usize,
) -> Option<StatementContext> {
    let mut i = stmt_idx + 1;
    let default_start = tokens.get(stmt_idx).map_or(0, |t| t.end);
    for kw in [Keyword::From, Keyword::Only] {
        if tokens.get(i).is_some_and(|t| t.is_keyword(kw)) {
            i += 1;
        }
    }
    let (target_table, consumed) = parse_table_name(tokens, i);
    i += consumed;

    let (_, where_start, ret_start) = scan_update_clauses(tokens, i, cursor_pos);
    let (in_returning, clause_start) = match () {
        _ if ret_start.is_some_and(|s| cursor_pos >= s) => (true, ret_start.unwrap()),
        _ if where_start.is_some_and(|s| cursor_pos >= s) => (false, where_start.unwrap()),
        _ => (false, default_start),
    };

    Some(StatementContext {
        stmt_type: StatementType::Delete,
        stmt_idx,
        depth,
        target_table,
        in_returning,
        in_set: false,
        in_column_list: false,
        clause_start,
    })
}
