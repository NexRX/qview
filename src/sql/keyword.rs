//! SQL keyword model used by the lightweight tokenizer / AST components.
//!
//! This module defines the minimal set of SQL keywords required for the current
//! autocomplete use‑cases. It intentionally omits many SQL keywords to keep the
//! surface area small and parsing lenient. Extend only when a new completion
//! context demands it.
//!
//! Design notes:
//! - Keywords are matched case‑insensitively via `from_lower` using a pre‑lower‑cased
//!   string slice.
//! - `as_str` provides a canonical lowercase representation (useful for display
//!   or debugging).
//! - The derived traits make it easy to compare, copy, and log values.
//!
//! Safety & Compatibility:
//! - Adding new variants is non‑breaking for downstream code that uses exhaustive
//!   matches with a wildcard (`_`). Code relying on exact matches should be
//!   reviewed when extending this enum.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
    Select,
    From,
    Join,
    On,
    As,
    Where,
    Group,
    Order,
    Limit,
    Offset,
    Union,
    Except,
    Intersect,
    With,
    Recursive,
    Only,
    Returning,
    Left,
    Right,
    Full,
    Outer,
    Inner,
    Cross,
    Natural,
    Lateral,
    Using,
    Values,
    // DML keywords
    Insert,
    Into,
    Update,
    Set,
    Delete,
    Default,
}

impl Keyword {
    /// Keywords that terminate the FROM clause table extraction.
    /// Note: ON is NOT a terminator - we handle it specially to support multi-table JOINs.
    pub const TERMINATORS: [Self; 8] = [
        Keyword::Where,
        Keyword::Group,
        Keyword::Order,
        Keyword::Limit,
        Keyword::Offset,
        Keyword::Union,
        Keyword::Except,
        Keyword::Intersect,
    ];

    /// Keywords that can appear before JOIN (join type modifiers).
    pub const JOIN_MODIFIERS: [Self; 7] = [
        Keyword::Left,
        Keyword::Right,
        Keyword::Full,
        Keyword::Outer,
        Keyword::Inner,
        Keyword::Cross,
        Keyword::Natural,
    ];

    /// Attempt to classify a *lower‑cased* word slice into a `Keyword`.
    /// Returns `None` if the word is not a recognized keyword.
    ///
    /// NOTE: The caller is responsible for lower‑casing the input. This avoids
    /// allocating new strings for each token; `to_ascii_lowercase` is typically
    /// performed once per identifier lexeme outside this function.
    pub fn from_lower(word: &str) -> Option<Self> {
        use Keyword::*;
        let kw = match word {
            "select" => Select,
            "from" => From,
            "join" => Join,
            "on" => On,
            "as" => As,
            "where" => Where,
            "group" => Group,
            "order" => Order,
            "limit" => Limit,
            "offset" => Offset,
            "union" => Union,
            "except" => Except,
            "intersect" => Intersect,
            "with" => With,
            "recursive" => Recursive,
            "only" => Only,
            "returning" => Returning,
            "left" => Left,
            "right" => Right,
            "full" => Full,
            "outer" => Outer,
            "inner" => Inner,
            "cross" => Cross,
            "natural" => Natural,
            "lateral" => Lateral,
            "using" => Using,
            "values" => Values,
            "insert" => Insert,
            "into" => Into,
            "update" => Update,
            "set" => Set,
            "delete" => Delete,
            "default" => Default,
            _ => return None,
        };
        Some(kw)
    }

    /// Canonical lowercase string form of the keyword.
    pub const fn as_str(self) -> &'static str {
        use Keyword::*;
        match self {
            Select => "select",
            From => "from",
            Join => "join",
            On => "on",
            As => "as",
            Where => "where",
            Group => "group",
            Order => "order",
            Limit => "limit",
            Offset => "offset",
            Union => "union",
            Except => "except",
            Intersect => "intersect",
            With => "with",
            Recursive => "recursive",
            Only => "only",
            Returning => "returning",
            Left => "left",
            Right => "right",
            Full => "full",
            Outer => "outer",
            Inner => "inner",
            Cross => "cross",
            Natural => "natural",
            Lateral => "lateral",
            Using => "using",
            Values => "values",
            Insert => "insert",
            Into => "into",
            Update => "update",
            Set => "set",
            Delete => "delete",
            Default => "default",
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_known_keywords() {
        for w in [
            "select",
            "from",
            "join",
            "on",
            "as",
            "where",
            "group",
            "order",
            "limit",
            "offset",
            "union",
            "except",
            "intersect",
            "with",
            "recursive",
            "only",
            "returning",
            "left",
            "right",
            "full",
            "outer",
            "inner",
            "cross",
            "natural",
            "lateral",
            "using",
            "values",
            "insert",
            "into",
            "update",
            "set",
            "delete",
            "default",
        ] {
            assert!(Keyword::from_lower(w).is_some(), "{w} should be recognized");
        }
    }

    #[test]
    fn rejects_unknown_words() {
        for w in ["foo", "bar", "random", "having", "window"] {
            assert!(
                Keyword::from_lower(w).is_none(),
                "{w} should NOT be recognized"
            );
        }
    }

    #[test]
    fn display_matches_as_str() {
        for kw in [
            Keyword::Select,
            Keyword::From,
            Keyword::Join,
            Keyword::On,
            Keyword::As,
            Keyword::Where,
            Keyword::Group,
            Keyword::Order,
            Keyword::Limit,
            Keyword::Offset,
            Keyword::Union,
            Keyword::Except,
            Keyword::Intersect,
            Keyword::With,
            Keyword::Recursive,
            Keyword::Only,
            Keyword::Returning,
            Keyword::Left,
            Keyword::Right,
            Keyword::Full,
            Keyword::Outer,
            Keyword::Inner,
            Keyword::Cross,
            Keyword::Natural,
            Keyword::Lateral,
            Keyword::Using,
            Keyword::Values,
            Keyword::Insert,
            Keyword::Into,
            Keyword::Update,
            Keyword::Set,
            Keyword::Delete,
            Keyword::Default,
        ] {
            assert_eq!(kw.to_string(), kw.as_str());
        }
    }
}
