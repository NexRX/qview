//! Internal types for SQL autocomplete suggestion.

use crate::Column;

/// Statement position info: (token_index, parenthesis_depth)
pub(super) type StmtPos = Option<(usize, i32)>;

/// Result of scanning for statement positions.
/// (last_select, last_insert, last_update, last_delete, returning_depth)
pub(super) type StatementPositions = (StmtPos, StmtPos, StmtPos, StmtPos, Option<i32>);

/// Represents a derived table or CTE with its projected columns.
#[derive(Debug, Clone)]
pub(super) struct DerivedTable {
    /// The alias for this derived table/CTE.
    pub alias: String,
    /// Columns projected by this derived table.
    /// If we can't determine the columns, this will be empty.
    pub columns: Vec<Column>,
}

/// Represents the type of DML statement being analyzed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
}

/// Context for a DML statement, containing the statement type and relevant position info.
#[derive(Debug)]
pub(super) struct StatementContext {
    pub stmt_type: StatementType,
    /// Index of the statement keyword (SELECT/INSERT/UPDATE/DELETE)
    #[allow(dead_code)]
    pub stmt_idx: usize,
    /// Parenthesis depth at the statement keyword
    #[allow(dead_code)]
    pub depth: i32,
    /// The table name for INSERT/UPDATE/DELETE statements
    pub target_table: Option<String>,
    /// Whether cursor is in RETURNING clause
    pub in_returning: bool,
    /// Whether cursor is in SET clause (UPDATE)
    pub in_set: bool,
    /// Whether cursor is in column list (INSERT)
    pub in_column_list: bool,
    /// Start position of the relevant clause for qualified prefix detection
    pub clause_start: usize,
}
