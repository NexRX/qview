//! Placeholder module for a future PostgreSQL AST implementation.
//!
//! This file exists so the crate's `reexport!(postgres_ast);` macro invocation
//! succeeds during compilation and tests. The real implementation can later
//! provide:
//! - Lightweight / error-tolerant parsing utilities for incomplete SQL
//! - Structures representing SELECT / FROM / JOIN clauses
//! - Helpers for cursor‑aware node lookup
//!
//! For now we only expose a minimal public API surface to avoid unused warnings
//! and to make incremental development straightforward.

/// A very small enum demonstrating how future AST node kinds might be
/// represented. Extend / replace once real parsing is introduced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AstNode {
    /// Represents a `SELECT` statement (possibly incomplete).
    Select,
    /// Represents a `FROM` clause with raw text captured.
    From(String),
    /// Generic / unknown fragment.
    Unknown(String),
}

impl AstNode {
    /// Convenience constructor for an unknown fragment.
    pub fn unknown<T: Into<String>>(raw: T) -> Self {
        AstNode::Unknown(raw.into())
    }
}

/// Parse returns a trivial `AstNode::Unknown` today. Replace with real logic later.
pub fn parse_fragment<T: Into<String>>(sql_fragment: T) -> AstNode {
    AstNode::unknown(sql_fragment)
}
