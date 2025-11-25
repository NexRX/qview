use crate::*;

/// An autocomplete suggestion. Variants represent different kinds of things that can be suggested while
/// the user types a SQL query: raw keywords, fully qualified columns and tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, derive_more::Display)]
pub enum Suggestion {
    #[display("{_0}")]
    Keyword(String),
    #[display("{_0}::{_1}")]
    Column(String, DataType),
    #[display("{schema}.{name}")]
    Table { schema: String, name: String },
}
pub type Suggestions = Vec<Suggestion>;

use crate::sql::{keyword::Keyword, token_kind::TokenKind, tokenizer::tokenize};

impl Suggestion {
    /// Search the SQL buffer for possible column suggestions at the given cursor.
    ///
    /// Strategy:
    /// 1. Tokenize the SQL.
    /// 2. Find the last `SELECT` token that appears before the cursor (track nesting).
    /// 3. From that `SELECT`, find the matching `FROM` at the same parenthesis depth.
    /// 4. Extract table names and their aliases from the range that follows.
    /// 5. If the cursor position represents a qualified prefix (`alias.`) only gather
    ///    columns for that single table; else gather columns for all tables in scope.
    pub async fn search(sql: &str, cursor: Cursor, meta: Database) -> Result<Suggestions> {
        let tokens = tokenize(sql);
        let cursor_pos = cursor.start();
        let (select_idx, select_depth) = match Self::locate_select(&tokens, cursor_pos) {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let from_idx = match Self::locate_from(&tokens, select_idx, select_depth) {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let (tables, aliases) = Self::extract_tables(&tokens, from_idx, select_depth);

        // Qualified prefix (e.g. users.)
        if let Some(prefix) = Self::qualified_prefix(sql, tokens[select_idx].end, cursor_pos) {
            let mut out = Vec::new();
            let base = aliases.get(&prefix).cloned().unwrap_or(prefix);
            Self::gather_columns(&meta, &base, &mut out).await;
            return Ok(out);
        }

        // Unqualified: aggregate columns from all tables in scope.
        let mut out = Vec::new();
        for tbl in tables {
            Self::gather_columns(&meta, &tbl, &mut out).await;
        }
        Ok(out)
    }

    /// Locate the index and parenthesis depth of the last `SELECT` token
    /// that starts before `cursor_pos`.
    ///
    /// Depth counting allows distinguishing nested subqueries: only tokens
    /// at the same depth as the matching `FROM` should be considered.
    fn locate_select(
        tokens: &[crate::sql::token::Token],
        cursor_pos: usize,
    ) -> Option<(usize, i32)> {
        let mut depth = 0;
        let mut last = None;
        for (idx, t) in tokens.iter().enumerate() {
            if t.start >= cursor_pos {
                break;
            }
            match t.kind {
                TokenKind::ParenOpen => depth += 1,
                TokenKind::ParenClose => depth -= 1,
                _ => {}
            }
            if t.is_keyword(Keyword::Select) {
                last = Some((idx, depth));
            }
        }
        last
    }

    /// From a previously found `SELECT` token, scan forward to find the
    /// corresponding `FROM` token at the same parenthesis depth.
    ///
    /// Returns the index of that `FROM` token if found.
    fn locate_from(
        tokens: &[crate::sql::token::Token],
        select_idx: usize,
        select_depth: i32,
    ) -> Option<usize> {
        let mut depth = select_depth;
        for (idx, t) in tokens.iter().enumerate().skip(select_idx + 1) {
            match t.kind {
                TokenKind::ParenOpen => depth += 1,
                TokenKind::ParenClose => depth -= 1,
                _ => {}
            }
            if depth == select_depth && t.is_keyword(Keyword::From) {
                return Some(idx);
            }
        }
        None
    }

    /// Extract table names and aliases beginning just after the `FROM` token.
    ///
    /// Parsing rules (simplified):
    /// - Continue until depth decreases below `select_depth` or a terminating
    ///   keyword (e.g. WHERE, GROUP, ORDER, etc.) at the same depth is found.
    /// - Handle comma separated tables and JOIN clauses, skipping the JOIN keyword.
    /// - Support aliases in the forms: `table AS alias` and `table alias`.
    fn extract_tables(
        tokens: &[crate::sql::token::Token],
        from_idx: usize,
        select_depth: i32,
    ) -> (Vec<String>, std::collections::HashMap<String, String>) {
        use std::collections::HashMap;
        let mut tables = Vec::new();
        let mut aliases = HashMap::new();
        let mut depth = select_depth;
        let mut i = from_idx + 1; // Start after the FROM token

        while let Some(t) = tokens.get(i) {
            // 1. Handle parenthesis tracking to respect nesting depth
            match t.kind {
                TokenKind::ParenOpen => {
                    depth += 1;
                    i += 1;
                    continue;
                }
                TokenKind::ParenClose => {
                    depth -= 1;
                    if depth < select_depth {
                        break; // Exit if we've closed out of our SELECT scope
                    }
                    i += 1;
                    continue;
                }
                _ => {}
            }

            // 2. Only process tokens at our target SELECT depth
            if depth != select_depth {
                i += 1;
                continue;
            }

            // 3. Handle terminating keywords and JOIN clauses
            if let TokenKind::Keyword(k) = &t.kind {
                if Keyword::TERMINATORS.contains(k) {
                    break; // Stop at WHERE, GROUP BY, ORDER BY, etc.
                }
                if *k == Keyword::Join {
                    i += 1;
                    continue; // Skip JOIN keyword itself
                }
            }

            // 4. Extract table names and handle aliasing patterns
            if let Some(name) = t.ident() {
                let name = name.to_string();
                if !tables.contains(&name) {
                    tables.push(name.clone());
                }

                // 5. Check for "table AS alias" pattern
                if let Some(alias_tok) = tokens
                    .get(i + 2)
                    .filter(|_| tokens.get(i + 1).is_some_and(|x| x.is_keyword(Keyword::As)))
                    .and_then(|x| x.ident())
                {
                    aliases.insert(alias_tok.to_string(), name.clone());
                    i += 3; // Skip table, AS, alias
                    continue;
                }

                // 6. Check for "table alias" pattern (no AS keyword)
                if let Some(alias_tok) = tokens
                    .get(i + 1)
                    .filter(|x| x.ident().is_some() && !matches!(x.kind, TokenKind::Keyword(_)))
                    .and_then(|x| x.ident())
                {
                    aliases.insert(alias_tok.to_string(), name.clone());
                    i += 2; // Skip table, alias
                    continue;
                }
            }

            // 7. Skip commas between table references
            if matches!(t.kind, TokenKind::Comma) {
                i += 1;
                continue;
            }
            i += 1;
        }
        (tables, aliases)
    }

    /// Determine a qualified table/alias prefix if the cursor is currently
    /// positioned after something like `alias.` within the SELECT projection.
    ///
    /// Returns the identifier (without the trailing dot) if present.
    fn qualified_prefix(sql: &str, select_end: usize, cursor_pos: usize) -> Option<String> {
        if cursor_pos <= select_end {
            return None;
        }
        let region = &sql[select_end..cursor_pos];
        region.rfind('.').and_then(|dot| {
            let before = region[..dot].trim_end();
            let ident = before
                .rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .unwrap_or("");
            (!ident.is_empty()).then(|| ident.to_string())
        })
    }

    /// Gather column suggestions for a single table name across all schemas.
    ///
    /// Columns are appended directly to `out` preserving order as supplied
    /// by `Table::ordered_columns`.
    async fn gather_columns(meta: &Database, table: &str, out: &mut Suggestions) {
        let schemas = meta.schemas.read().await;
        for schema in schemas.values() {
            let tables = schema.tables.read().await;
            if let Some(t) = tables.get(table) {
                for (col, dt) in t.ordered_columns().await {
                    out.push(Suggestion::Column(col, dt));
                }
            }
        }
    }
}
