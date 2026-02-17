//! SQL autocomplete suggestion module.
//!
//! This module provides column suggestions based on SQL cursor position
//! and database metadata.

mod context;
mod cte;
mod from_clause;
mod helpers;
mod search;
mod suggestion_tests;
mod types;

use crate::sql::tokenizer::tokenize;
use crate::{Column, Cursor, Database, Result};

/// An autocomplete suggestion. Variants represent different kinds of things that can be suggested
/// while the user types a SQL query: raw keywords, fully qualified columns and tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, derive_more::Display)]
pub enum Suggestion {
    #[display("{_0}")]
    Keyword(String),
    #[display("{}::{}", _0.name, _0.data_type)]
    Column(Column),
    #[display("{schema}.{name}")]
    Table { schema: String, name: String },
}

pub type Suggestions = Vec<Suggestion>;

impl Suggestion {
    /// Search the SQL buffer for possible column suggestions at the given cursor.
    pub async fn search(sql: &str, cursor: Cursor, meta: Database) -> Result<Suggestions> {
        let tokens = tokenize(sql);
        let cursor_pos = cursor.start();
        search::search(sql, &tokens, cursor_pos, &meta).await
    }
}
