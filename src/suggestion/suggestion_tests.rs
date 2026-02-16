//! Autocomplete suggestion tests using rstest case-driven testing.
//!
//! This module tests the `Suggestion::search` functionality which provides
//! column suggestions based on SQL cursor position and database metadata.
//!
//! All tests cases must have a comment with the case number followed by a
//! short description.
//!
//! Tests are organized into logical groups:
//! - Basic unqualified column suggestions
//! - Subquery isolation (depth tracking)
//! - Qualified column suggestions (e.g., `table.column`)
//! - Known gaps/limitations in the current implementation
//! - Union, CTE, and set operation handling
//! - Edge cases and PostgreSQL-specific grammar

#![cfg(test)]
#![allow(clippy::type_complexity)]

use crate::*;
use rstest::rstest;

/// Column definition: (column_name, data_type)
type ColumnDef<'a> = (&'a str, DataType);

/// Table definition: (table_name, columns)
type TableDef<'a> = (&'a str, Vec<ColumnDef<'a>>);

/// Build a `Database` with tables across one or two schemas.
///
/// When `other_schema` is `None`, all tables go into "public".
/// When `other_schema` is `Some((schema_name, tables))`, those tables
/// are added to the specified schema in addition to public tables.
async fn database(
    database_name: &str,
    public_tables: &[TableDef<'_>],
    other_schema: Option<(&str, &[TableDef<'_>])>,
) -> Database {
    let mut meta = Database::new(database_name);

    for (table_name, columns) in public_tables {
        meta.insert_table(
            "public",
            Table::new_with_ordered(
                *table_name,
                columns
                    .iter()
                    .cloned()
                    .map(|(name, data_type)| (name.to_string(), data_type)),
            ),
        )
        .await;
    }

    if let Some((schema_name, tables)) = other_schema {
        for (table_name, columns) in tables {
            meta.insert_table(
                schema_name,
                Table::new_with_ordered(
                    *table_name,
                    columns
                        .iter()
                        .cloned()
                        .map(|(name, data_type)| (name.to_string(), data_type)),
                ),
            )
            .await;
        }
    }

    meta
}

/// Shorthand for single-schema database setup.
async fn db(tables: &[TableDef<'_>]) -> Database {
    database("postgres", tables, None).await
}

#[cfg(test)]
mod unqualified_column_suggestions {
    //! Tests for unqualified column suggestions (no table prefix).
    //!
    //! These tests verify that when the cursor is in a SELECT projection
    //! without a qualified prefix, all columns from in-scope tables are suggested.

    use super::*;

    #[rstest]
    // Case 1: single_table_single_column - Basic case with one table and one column
    #[case::single_table_single_column(
        "SELECT  FROM example",
        7,
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 2: single_table_multiple_columns - Single table with multiple columns
    #[case::single_table_multiple_columns(
        "SELECT  FROM example",
        7,
        vec![("example", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 3: column_order_preserved - Verify column order matches declaration order
    #[case::column_order_preserved(
        "SELECT  FROM ord",
        7,
        vec![("ord", vec![("id", DataType::Uuid), ("created_at", DataType::Text(None)), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("created_at", DataType::Text(None)), ("name", DataType::Text(None))]
    )]
    // Case 4: comma_separated_tables - Multiple tables joined with comma
    #[case::comma_separated_tables(
        "SELECT  FROM example, users",
        7,
        vec![
            ("example", vec![("id", DataType::Uuid)]),
            ("users", vec![("user_id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("user_id", DataType::Uuid), ("email", DataType::Text(None))]
    )]
    // Case 5: duplicate_column_names_across_tables - Same column names in different tables
    #[case::duplicate_column_names_across_tables(
        "SELECT  FROM a, b",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
        ]
    )]
    // Case 6: simple_join - Basic JOIN with ON clause
    #[case::simple_join(
        "SELECT  FROM example JOIN users ON example.id = users.example_id",
        7,
        vec![
            ("example", vec![("id", DataType::Uuid)]),
            ("users", vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("user_id", DataType::Uuid), ("example_id", DataType::Uuid)]
    )]
    // Case 7: join_with_aliases - JOIN with AS aliases on both tables
    #[case::join_with_aliases(
        "SELECT  FROM a AS x JOIN b AS y ON x.id = y.id",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
        ]
    )]
    // Case 8: join_chain_with_where - JOIN followed by WHERE terminator
    #[case::join_chain_with_where(
        "SELECT  FROM a JOIN b ON a.id = b.id WHERE a.id > 0",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
        ]
    )]
    // Case 9: using_clause - JOIN with USING clause
    #[case::using_clause(
        "SELECT  FROM a JOIN b USING(id)",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    // Case 10: natural_join - NATURAL JOIN captures both tables
    #[case::natural_join(
        "SELECT  FROM a NATURAL JOIN b",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    // Case 11: cross_join - CROSS JOIN captures both tables
    #[case::cross_join(
        "SELECT  FROM a CROSS JOIN b",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    // Case 12: alias_without_as - Table alias without AS keyword
    #[case::alias_without_as(
        "SELECT  FROM example e",
        7,
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 13: alias_with_as - Table alias with AS keyword
    #[case::alias_with_as(
        "SELECT  FROM example AS ex",
        7,
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 14: where_terminator - WHERE stops table extraction
    #[case::where_terminator(
        "SELECT  FROM example WHERE example.id IS NOT NULL",
        7,
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 15: group_by_terminator - GROUP BY stops table extraction
    #[case::group_by_terminator(
        "SELECT  FROM example GROUP BY example.id",
        7,
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 16: order_by_terminator - ORDER BY stops table extraction
    #[case::order_by_terminator(
        "SELECT  FROM a ORDER BY a.id",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 17: limit_terminator - LIMIT stops table extraction
    #[case::limit_terminator(
        "SELECT  FROM a LIMIT 10",
        7,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 18: no_from_clause - No FROM clause yields no suggestions
    #[case::no_from_clause(
        "SELECT 1",
        7,
        vec![],
        vec![]
    )]
    // Case 19: cursor_before_from - Cursor before FROM yields no suggestions
    #[case::cursor_before_from(
        "SELECT  foo",
        7,
        vec![],
        vec![]
    )]
    // Case 20: unknown_table - Unknown table in FROM yields no suggestions
    #[case::unknown_table(
        "SELECT  FROM missing",
        7,
        vec![],
        vec![]
    )]
    // Case 21: trailing_comma_after_table - Trailing comma after table list
    #[case::trailing_comma_after_table(
        "SELECT  FROM a,",
        7,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 22: cursor_in_second_statement - Cursor in second SQL statement
    #[case::cursor_in_second_statement(
        "SELECT  FROM a; SELECT  FROM b",
        23,
        vec![("b", vec![("bid", DataType::Uuid)])],
        vec![("bid", DataType::Uuid)]
    )]
    // Case 23: self_join - Same table joined to itself with different aliases (table deduplicated)
    #[case::self_join(
        "SELECT  FROM users a JOIN users b ON a.manager_id = b.id",
        7,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("manager_id", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("manager_id", DataType::Uuid)]
    )]
    // Case 24: double_join - Two tables joined (ON terminates at first ON, so only a and b captured)
    #[case::double_join(
        "SELECT  FROM a JOIN b ON a.id = b.a_id",
        7,
        vec![
            ("a", vec![("aid", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("bid", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![
            ("aid", DataType::Uuid), ("name", DataType::Text(None)),
            ("bid", DataType::Uuid), ("a_id", DataType::Uuid)
        ]
    )]
    // Case 25: left_outer_join - LEFT OUTER JOIN syntax
    #[case::left_outer_join(
        "SELECT  FROM a LEFT OUTER JOIN b ON a.id = b.a_id",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("a_id", DataType::Uuid)
        ]
    )]
    // Case 26: right_join - RIGHT JOIN syntax
    #[case::right_join(
        "SELECT  FROM a RIGHT JOIN b ON a.id = b.a_id",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("a_id", DataType::Uuid)
        ]
    )]
    // Case 27: full_outer_join - FULL OUTER JOIN syntax
    #[case::full_outer_join(
        "SELECT  FROM a FULL OUTER JOIN b ON a.id = b.a_id",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("a_id", DataType::Uuid)
        ]
    )]
    // Case 28: subquery_in_where - Subquery in WHERE doesn't affect FROM scope
    #[case::subquery_in_where(
        "SELECT  FROM users WHERE id IN (SELECT user_id FROM orders)",
        7,
        vec![
            ("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("orders", vec![("id", DataType::Uuid), ("user_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 29: subquery_in_select_list - Scalar subquery in SELECT list
    #[case::subquery_in_select_list(
        "SELECT  , (SELECT MAX(id) FROM other) FROM users",
        7,
        vec![
            ("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("other", vec![("id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 30: exists_subquery - EXISTS subquery doesn't pollute outer scope
    #[case::exists_subquery(
        "SELECT  FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 31: having_clause - HAVING terminates like WHERE
    #[case::having_clause(
        "SELECT  FROM a GROUP BY a.category HAVING COUNT(*) > 1",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("category", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("category", DataType::Text(None))]
    )]
    // Case 32: distinct_keyword - DISTINCT doesn't affect suggestions
    #[case::distinct_keyword(
        "SELECT DISTINCT  FROM a",
        16,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 33: multiple_columns_in_select - Cursor after existing columns
    #[case::multiple_columns_in_select(
        "SELECT id, name,  FROM a",
        17,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
    )]
    // Case 34: between_operator - BETWEEN in WHERE clause
    #[case::between_operator(
        "SELECT  FROM a WHERE a.id BETWEEN 1 AND 100",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 35: in_list_operator - IN operator with list
    #[case::in_list_operator(
        "SELECT  FROM a WHERE a.status IN ('active', 'pending')",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("status", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("status", DataType::Text(None))]
    )]
    // Case 36: like_operator - LIKE pattern matching
    #[case::like_operator(
        "SELECT  FROM a WHERE a.name LIKE '%test%'",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 37: complex_where_and_or - Complex WHERE with AND/OR
    #[case::complex_where_and_or(
        "SELECT  FROM a WHERE (a.x > 1 AND a.y < 10) OR a.z = 5",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid), ("z", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid), ("z", DataType::Uuid)]
    )]
    // Case 38: offset_clause - OFFSET terminates like LIMIT
    #[case::offset_clause(
        "SELECT  FROM a LIMIT 10 OFFSET 20",
        7,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 39: for_update_clause - FOR UPDATE locking clause
    #[case::for_update_clause(
        "SELECT  FROM a FOR UPDATE",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 40: extra_whitespace - Extra whitespace handling
    #[case::extra_whitespace(
        "SELECT    FROM   a",
        10,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 41: mixed_case_identifiers - Mixed case table/column names
    #[case::mixed_case_identifiers(
        "SELECT  FROM Users",
        7,
        vec![("Users", vec![("Id", DataType::Uuid), ("Name", DataType::Text(None))])],
        vec![("Id", DataType::Uuid), ("Name", DataType::Text(None))]
    )]
    // Case 42: table_alias_matches_column_name - Alias same as a column name
    #[case::table_alias_matches_column_name(
        "SELECT  FROM users id",
        7,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 43: triple_join_chain - Three tables joined in sequence (now works!)
    #[case::triple_join_chain(
        "SELECT  FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
        7,
        vec![
            ("a", vec![("aid", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("bid", DataType::Uuid), ("a_id", DataType::Uuid)]),
            ("c", vec![("cid", DataType::Uuid), ("b_id", DataType::Uuid)])
        ],
        vec![
            ("aid", DataType::Uuid), ("name", DataType::Text(None)),
            ("bid", DataType::Uuid), ("a_id", DataType::Uuid),
            ("cid", DataType::Uuid), ("b_id", DataType::Uuid)
        ]
    )]
    // Case 44: four_table_join - Four tables in join chain (now works!)
    #[case::four_table_join(
        "SELECT  FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id JOIN d ON c.id = d.c_id",
        7,
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("a_id", DataType::Uuid)]),
            ("c", vec![("cid", DataType::Uuid), ("b_id", DataType::Uuid)]),
            ("d", vec![("did", DataType::Uuid), ("c_id", DataType::Uuid)])
        ],
        vec![
            ("aid", DataType::Uuid),
            ("bid", DataType::Uuid), ("a_id", DataType::Uuid),
            ("cid", DataType::Uuid), ("b_id", DataType::Uuid),
            ("did", DataType::Uuid), ("c_id", DataType::Uuid)
        ]
    )]
    // Case 45: mixed_join_types - Mixed LEFT and INNER joins (now works!)
    #[case::mixed_join_types(
        "SELECT  FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
        7,
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("a_id", DataType::Uuid)]),
            ("c", vec![("cid", DataType::Uuid), ("b_id", DataType::Uuid)])
        ],
        vec![
            ("aid", DataType::Uuid),
            ("bid", DataType::Uuid), ("a_id", DataType::Uuid),
            ("cid", DataType::Uuid), ("b_id", DataType::Uuid)
        ]
    )]
    // Case 46: except_operator - EXCEPT isolates first SELECT
    #[case::except_operator(
        "SELECT  FROM a EXCEPT SELECT * FROM b",
        7,
        vec![("a", vec![("aid", DataType::Uuid)]), ("b", vec![("bid", DataType::Uuid)])],
        vec![("aid", DataType::Uuid)]
    )]
    #[tokio::test]
    async fn should_suggest_columns(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("suggestion search should not error");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(result, expected_columns);
    }
}

#[cfg(test)]
mod subquery_isolation {
    //! Tests for subquery isolation via parenthesis depth tracking.
    //!
    //! These ensure that nested SELECTs only see tables from their own scope,
    //! and outer queries don't see inner subquery tables.

    use super::*;

    #[rstest]
    // Case 1: inner_select_sees_inner_table - Inner SELECT sees only inner table
    #[case::inner_select_sees_inner_table(
        "SELECT (SELECT  FROM example) FROM other",
        15,
        vec![("example", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 2: deeply_nested_subquery - Nested subquery, cursor in inner SELECT
    #[case::deeply_nested_subquery(
        "SELECT * FROM (SELECT  FROM inner_tbl) sub",
        22,
        vec![("inner_tbl", vec![("iid", DataType::Uuid)])],
        vec![("iid", DataType::Uuid)]
    )]
    // Case 3: subquery_with_join_chain - Inner SELECT with JOIN sees joined tables
    #[case::subquery_with_join_chain(
        "SELECT * FROM (SELECT  FROM inner_tbl JOIN another ON inner_tbl.id = another.inner_id) sub",
        21,
        vec![
            ("inner_tbl", vec![("id", DataType::Uuid)]),
            ("another", vec![("inner_id", DataType::Uuid), ("val", DataType::Text(None))]),
            ("outer", vec![("oid", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("inner_id", DataType::Uuid), ("val", DataType::Text(None))]
    )]
    // Case 4: outer_select_excludes_inner_tables - Scalar subquery in SELECT list doesn't leak tables
    #[case::outer_select_excludes_inner_tables(
        "SELECT  , (SELECT id FROM inner_tbl) FROM outer_tbl",
        7,
        vec![
            ("outer_tbl", vec![("oid", DataType::Uuid), ("name", DataType::Text(None))]),
            ("inner_tbl", vec![("id", DataType::Uuid)])
        ],
        vec![("oid", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 5: deep_nesting_isolation - Cursor in middle level of nesting
    #[case::deep_nesting_isolation(
        "SELECT * FROM (SELECT  FROM middle WHERE x IN (SELECT id FROM deep)) sub",
        22,
        vec![
            ("middle", vec![("mid", DataType::Uuid), ("x", DataType::Uuid)]),
            ("deep", vec![("id", DataType::Uuid)])
        ],
        vec![("mid", DataType::Uuid), ("x", DataType::Uuid)]
    )]
    // Case 6: subquery_in_where_clause - Subquery in WHERE doesn't leak to outer
    #[case::subquery_in_where_clause(
        "SELECT  FROM a WHERE a.id IN (SELECT b.a_id FROM b)",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 7: correlated_subquery - Correlated subquery isolation
    #[case::correlated_subquery(
        "SELECT  FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 8: multiple_subqueries_in_select - Multiple scalar subqueries in SELECT
    #[case::multiple_subqueries_in_select(
        "SELECT  , (SELECT MAX(id) FROM b), (SELECT MIN(id) FROM c) FROM a",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid)]),
            ("c", vec![("id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 9: subquery_in_from_outer_cursor - Cursor in outer SELECT with derived table (sees derived columns)
    #[case::subquery_in_from_outer_cursor(
        "SELECT  FROM (SELECT id FROM inner_tbl) sub, outer_tbl",
        7,
        vec![
            ("inner_tbl", vec![("id", DataType::Uuid)]),
            ("outer_tbl", vec![("oid", DataType::Uuid), ("name", DataType::Text(None))])
        ],
        vec![("id", DataType::Unknown), ("oid", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 10: subquery_comparison_operator - Subquery with comparison operator
    #[case::subquery_comparison_operator(
        "SELECT  FROM a WHERE a.value > (SELECT AVG(b.value) FROM b)",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("value", DataType::Uuid)]),
            ("b", vec![("id", DataType::Uuid), ("value", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("value", DataType::Uuid)]
    )]
    // Case 11: nested_exists_subquery - Nested EXISTS subqueries
    #[case::nested_exists_subquery(
        "SELECT  FROM a WHERE EXISTS (SELECT 1 FROM b WHERE EXISTS (SELECT 1 FROM c WHERE c.b_id = b.id))",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid)]),
            ("c", vec![("id", DataType::Uuid), ("b_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 12: subquery_in_case_when - Subquery inside CASE WHEN
    #[case::subquery_in_case_when(
        "SELECT  FROM a WHERE CASE WHEN (SELECT COUNT(*) FROM b) > 0 THEN true ELSE false END",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 13: union_in_subquery - UNION inside subquery doesn't leak
    #[case::union_in_subquery(
        "SELECT  FROM a WHERE a.id IN (SELECT id FROM b UNION SELECT id FROM c)",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid)]),
            ("c", vec![("id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 14: lateral_subquery_isolation - LATERAL subquery sees outer table but cursor in outer sees outer table
    #[case::lateral_subquery_isolation(
        "SELECT  FROM a LEFT JOIN LATERAL (SELECT b.id AS bid FROM b WHERE b.a_id = a.id) sub ON true",
        7,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![("bid", DataType::Unknown), ("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_isolate_subquery_scope(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("suggestion search should not error");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(
            result, expected_columns,
            "subquery isolation failed: columns from wrong depth leaked"
        );
    }
}

#[cfg(test)]
mod qualified_column_suggestions {
    //! Tests for qualified column suggestions (e.g., `table.` or `alias.`).
    //!
    //! When the cursor follows a qualified prefix, only columns from that
    //! specific table or alias should be suggested.

    use super::*;

    #[rstest]
    // Case 1: qualified_by_table_name - Qualified by actual table name
    #[case::qualified_by_table_name(
        "SELECT users.  FROM example JOIN users ON example.id = users.example_id",
        13,
        vec![
            ("example", vec![("id", DataType::Uuid)]),
            ("users", vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)])
        ],
        vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)]
    )]
    // Case 2: qualified_same_column_names - Qualified with same column names in both tables
    #[case::qualified_same_column_names(
        "SELECT a.  FROM a JOIN b ON a.id = b.id",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 3: alias_simple - Simple alias without AS keyword
    #[case::alias_simple(
        "SELECT ex.  FROM example ex",
        10,
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 4: alias_with_as_keyword - Alias using AS keyword
    #[case::alias_with_as_keyword(
        "SELECT x.  FROM a AS x JOIN b AS y ON x.id = y.id",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 5: alias_with_where_terminator - Alias followed by WHERE clause
    #[case::alias_with_where_terminator(
        "SELECT x.  FROM a AS x WHERE x.id > 0",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 6: unknown_alias_yields_none - Unknown alias returns no suggestions
    #[case::unknown_alias_yields_none(
        "SELECT z.  FROM a AS x",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![]
    )]
    // Case 7: simple_table_prefix - Simple table name as prefix
    #[case::simple_table_prefix(
        "SELECT a.  FROM a",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 8: qualified_without_from_yields_none - No FROM clause with qualified prefix
    #[case::qualified_without_from_yields_none(
        "SELECT a.",
        9,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![]
    )]
    // Case 9: qualified_with_order_by - Qualified prefix with ORDER BY terminator
    #[case::qualified_with_order_by(
        "SELECT a.  FROM a ORDER BY a.id",
        9,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 10: qualified_inside_subquery_no_outer_leak - Outer alias not visible in subquery
    #[case::qualified_inside_subquery_no_outer_leak(
        "SELECT (SELECT o.  FROM inner) FROM outer o",
        18,
        vec![
            ("outer", vec![("oid", DataType::Uuid), ("oname", DataType::Text(None))]),
            ("inner", vec![("iid", DataType::Uuid), ("ival", DataType::Text(None))])
        ],
        vec![]
    )]
    // Case 11: qualified_inner_table_in_subquery - Qualified inner table within subquery
    #[case::qualified_inner_table_in_subquery(
        "SELECT (SELECT inner.  FROM inner JOIN another ON inner.id = another.inner_id) FROM outer",
        24,
        vec![
            ("inner", vec![("id", DataType::Uuid)]),
            ("another", vec![("inner_id", DataType::Uuid), ("val", DataType::Text(None))]),
            ("outer", vec![("oid", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid)]
    )]
    // Case 12: using_clause_qualified - Qualified prefix with USING clause
    #[case::using_clause_qualified(
        "SELECT a.  FROM a JOIN b USING(id)",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 13: numeric_literal_dot_disambiguation - Disambiguate from numeric literal dot
    #[case::numeric_literal_dot_disambiguation(
        "SELECT COALESCE(u. , 1.0) FROM users u",
        18,
        vec![("users", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("email", DataType::Text(None))]
    )]
    // Case 14: self_join_qualified_first_alias - Self-join qualified by first alias
    #[case::self_join_qualified_first_alias(
        "SELECT a.  FROM users a JOIN users b ON a.manager_id = b.id",
        9,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("manager_id", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("manager_id", DataType::Uuid)]
    )]
    // Case 15: self_join_qualified_second_alias - Self-join qualified by second alias
    #[case::self_join_qualified_second_alias(
        "SELECT b.  FROM users a JOIN users b ON a.manager_id = b.id",
        9,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("manager_id", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("manager_id", DataType::Uuid)]
    )]
    // Case 16: qualified_in_case_expression - Qualified prefix inside CASE expression
    #[case::qualified_in_case_expression(
        "SELECT CASE WHEN a.id > 0 THEN a.  ELSE 'none' END FROM a",
        34,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 17: qualified_in_function_arg - Qualified prefix as function argument
    #[case::qualified_in_function_arg(
        "SELECT UPPER(a. ) FROM a",
        14,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 18: qualified_in_arithmetic - Qualified prefix in arithmetic expression
    #[case::qualified_in_arithmetic(
        "SELECT a.  * 100 FROM a",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("price", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("price", DataType::Uuid)]
    )]
    // Case 19: qualified_after_comma - Qualified prefix after other columns
    #[case::qualified_after_comma(
        "SELECT id, name, a.  FROM a",
        20,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
    )]
    // Case 20: qualified_with_left_join - Qualified prefix with LEFT JOIN
    #[case::qualified_with_left_join(
        "SELECT a.  FROM a LEFT JOIN b ON a.id = b.a_id",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 21: qualified_triple_join_middle_table - Qualified middle table in join chain
    #[case::qualified_triple_join_middle_table(
        "SELECT b.  FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid)]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("c", vec![("id", DataType::Uuid), ("b_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("a_id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 22: qualified_in_concat - Qualified prefix in string concatenation
    #[case::qualified_in_concat(
        "SELECT a.  || ' suffix' FROM a",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 23: qualified_in_cast - Qualified prefix with type cast
    #[case::qualified_in_cast(
        "SELECT a. ::text FROM a",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("count", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("count", DataType::Uuid)]
    )]
    // Case 24: qualified_mixed_case_alias - Mixed case alias
    #[case::qualified_mixed_case_alias(
        "SELECT MyAlias.  FROM users MyAlias",
        15,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 25: qualified_single_char_alias - Single character alias
    #[case::qualified_single_char_alias(
        "SELECT x.  FROM very_long_table_name x",
        9,
        vec![("very_long_table_name", vec![("id", DataType::Uuid), ("data", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("data", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_suggest_qualified_columns(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("suggestion search should not error");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(
            result, expected_columns,
            "qualified suggestions should only include columns from the referenced table/alias"
        );
    }

    #[rstest]
    // Case 1: alias_shadows_real_table - Alias with same name as real table resolves to alias
    #[case::alias_shadows_real_table(
        "SELECT fake.  FROM real AS fake, fake",
        12,
        vec![
            ("real", vec![("rid", DataType::Uuid), ("rval", DataType::Text(None))]),
            ("fake", vec![("fid", DataType::Uuid)])
        ],
        vec![("rid", DataType::Uuid), ("rval", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_prefer_alias_over_same_named_table(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("alias shadowing resolution");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(
            result, expected_columns,
            "alias shadowing: qualified alias should resolve to aliased table, not same-named table"
        );
    }
}

#[cfg(test)]
mod union_and_set_operations {
    //! Tests for UNION, INTERSECT, EXCEPT, and CTE handling.
    //!
    //! Each SELECT in a set operation should only see its own FROM tables.

    use super::*;

    #[rstest]
    // Case 1: union_first_select - First SELECT in UNION sees only first table
    #[case::union_first_select(
        "SELECT  FROM a UNION SELECT  FROM b",
        7,
        vec![("a", vec![("aid", DataType::Uuid)]), ("b", vec![("bid", DataType::Uuid)])],
        vec![("aid", DataType::Uuid)]
    )]
    // Case 2: union_second_select - Second SELECT in UNION sees only second table
    #[case::union_second_select(
        "SELECT * FROM a UNION SELECT  FROM b",
        29,
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))])
        ],
        vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))]
    )]
    // Case 3: intersect_first_select - First SELECT in INTERSECT sees only first table
    #[case::intersect_first_select(
        "SELECT  FROM a INTERSECT SELECT  FROM b",
        7,
        vec![("a", vec![("aid", DataType::Uuid)]), ("b", vec![("bid", DataType::Uuid)])],
        vec![("aid", DataType::Uuid)]
    )]
    // Case 4: intersect_second_select - Second SELECT in INTERSECT sees only second table
    #[case::intersect_second_select(
        "SELECT  FROM a INTERSECT SELECT  FROM b",
        32,
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))])
        ],
        vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))]
    )]
    // Case 5: qualified_union_second_select - Qualified prefix in second UNION SELECT
    #[case::qualified_union_second_select(
        "SELECT aid FROM a UNION SELECT b.  FROM b",
        29,
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))])
        ],
        vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_isolate_set_operation_scope(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("set operation scope");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(
            result, expected_columns,
            "set operation should isolate each SELECT's scope"
        );
    }
}

#[cfg(test)]
mod multi_schema {
    //! Tests for multi-schema table resolution.
    //!
    //! When the same table name exists in multiple schemas, columns from
    //! all matching tables should be aggregated.

    use super::*;

    #[rstest]
    // Case 1: duplicate_table_across_schemas - Same table name in public and analytics schemas
    // BTreeMap orders schemas alphabetically: "analytics" comes before "public"
    #[case::duplicate_table_across_schemas(
        "SELECT  FROM users",
        7,
        vec![("users", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])],
        "analytics",
        vec![("users", vec![("user_id", DataType::Uuid), ("created_at", DataType::Text(None))])],
        vec![
            ("user_id", DataType::Uuid), ("created_at", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    #[tokio::test]
    async fn should_aggregate_multi_schema_columns(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] public_tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] other_schema: &str,
        #[case] other_tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database(
            "postgres",
            &public_tables,
            Some((other_schema, &other_tables)),
        )
        .await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("multi-schema aggregation");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(
            result, expected_columns,
            "multi-schema duplicate table columns should aggregate in schema insertion order"
        );
    }
}

#[cfg(test)]
mod known_gaps {
    //! Tests documenting known gaps/limitations in the current implementation.
    //!
    //! These tests verify the *current* behavior even when it's incomplete.
    //! When functionality is added, these tests should be updated to expect
    //! the new correct behavior.

    use super::*;

    #[rstest]
    // Case 1: derived_star_not_expanded - Star (*) in derived subquery not expanded
    #[case::derived_star_not_expanded(
        "SELECT  FROM (SELECT * FROM a) sub",
        7,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![],
        "star (*) in derived subquery not expanded"
    )]
    // Case 2: derived_column_aliases_resolved - Column aliases in derived subquery ARE now resolved
    #[case::derived_column_aliases_resolved(
        "SELECT sub.  FROM (SELECT id AS ident, name AS nm FROM a) sub",
        12,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("ident", DataType::Unknown), ("nm", DataType::Unknown)],
        "derived column aliases now exposed"
    )]
    // Case 3: qualified_derived_star_not_expanded - Qualified derived star prefix not expanded
    #[case::qualified_derived_star_not_expanded(
        "SELECT sub.  FROM (SELECT * FROM a) sub",
        12,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![],
        "qualified derived star not expanded"
    )]
    // Case 4: parenthesized_join_alias_not_recognized - Parenthesized join group alias not recognized
    #[case::parenthesized_join_alias_not_recognized(
        "SELECT ab.  FROM (a JOIN b ON a.aid = b.bid) ab",
        11,
        vec![("a", vec![("aid", DataType::Uuid)]), ("b", vec![("bid", DataType::Uuid)])],
        vec![],
        "parenthesized join group alias not recognized"
    )]
    // Case 5: quoted_identifiers_supported - Quoted identifiers ARE now recognized
    #[case::quoted_identifiers_supported(
        "SELECT ua.  FROM \"User Accounts\" AS ua",
        11,
        vec![("User Accounts", vec![("userid", DataType::Uuid), ("display_name", DataType::Text(None))])],
        vec![("userid", DataType::Uuid), ("display_name", DataType::Text(None))],
        "quoted identifiers now recognized"
    )]
    // Case 6: values_derived_alias_not_supported - VALUES derived alias columns not exposed
    #[case::values_derived_alias_not_supported(
        "SELECT v.  FROM (VALUES (1), (2)) AS v(x)",
        10,
        vec![],
        vec![],
        "VALUES derived alias columns not exposed"
    )]
    // Case 7: function_as_table_not_supported - Function/table functions not resolved
    #[case::function_as_table_not_supported(
        "SELECT f.  FROM pg_catalog.generate_series(1,10) AS f(x)",
        10,
        vec![],
        vec![],
        "function/table functions not resolved"
    )]
    // Case 8: array_subscript_supported - Array subscript access with alias now works
    #[case::array_subscript_supported(
        "SELECT a.  FROM (SELECT tags[1] AS tag FROM items) a",
        10,
        vec![("items", vec![("id", DataType::Uuid), ("tags", DataType::Text(None))])],
        vec![("tag", DataType::Unknown)],
        "array subscript with alias in derived table now works"
    )]
    // Case 9: json_operator_supported - JSON operators with alias now work
    #[case::json_operator_supported(
        "SELECT j.  FROM (SELECT data->>'key' AS val FROM items) j",
        10,
        vec![("items", vec![("id", DataType::Uuid), ("data", DataType::Text(None))])],
        vec![("val", DataType::Unknown)],
        "JSON operators with alias in derived table now work"
    )]
    // Case 11: returning_clause_now_supported - RETURNING clause now works
    #[case::returning_clause_now_supported(
        "INSERT INTO users (name) VALUES ('test') RETURNING ",
        50,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))],
        "RETURNING clause now works for INSERT statements"
    )]
    // Case 12: with_recursive_cte_works - WITH RECURSIVE CTE columns now resolved
    #[case::with_recursive_cte_works(
        "WITH RECURSIVE tree AS (SELECT id FROM nodes UNION ALL SELECT n.id FROM nodes n JOIN tree t ON n.parent_id = t.id) SELECT  FROM tree",
        125,
        vec![("nodes", vec![("id", DataType::Uuid), ("parent_id", DataType::Uuid)])],
        vec![("id", DataType::Unknown)],
        "WITH RECURSIVE CTE now resolves columns"
    )]
    // Case 13: only_keyword_works - ONLY keyword for inheritance now works
    #[case::only_keyword_works(
        "SELECT  FROM ONLY parent_table",
        7,
        vec![("parent_table", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)],
        "ONLY keyword now works"
    )]
    #[tokio::test]
    async fn should_document_known_gap(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
        #[case] gap_description: &str,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("known gap test");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(result, expected_columns, "gap: {gap_description}");
    }

    #[rstest]
    // Case 1: cte_chain_only_base_table - CTE chain not exposed, only base tables available
    #[case::cte_chain_only_base_table(
        "WITH x AS (SELECT id FROM a), y AS (SELECT id FROM x) SELECT  FROM a",
        61,
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)],
        "CTE chain columns not exposed; only base tables available"
    )]
    #[tokio::test]
    async fn should_document_cte_gap(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
        #[case] gap_description: &str,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("CTE gap test");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(result, expected_columns, "gap: {gap_description}");
    }
}

#[cfg(test)]
mod postgres_edge_cases {
    //! PostgreSQL-specific grammar edge cases.
    //!
    //! These test advanced PostgreSQL syntax like LATERAL, WINDOW, etc.

    use super::*;

    #[rstest]
    // Case 1: lateral_join_qualified - LATERAL join with qualified prefix
    #[case::lateral_join_qualified(
        "SELECT a.  FROM a LEFT JOIN LATERAL (SELECT id FROM b WHERE b.id = a.id) AS bl ON true",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 2: window_clause_qualified - WINDOW clause with qualified prefix
    #[case::window_clause_qualified(
        "SELECT a.  FROM a WINDOW w AS (PARTITION BY a.id)",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 3: row_number_window_function - Window function in SELECT
    #[case::row_number_window_function(
        "SELECT a. , ROW_NUMBER() OVER (ORDER BY a.id) FROM a",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 4: aggregate_with_filter - Aggregate function with FILTER clause
    #[case::aggregate_with_filter(
        "SELECT a. , COUNT(*) FILTER (WHERE a.active) FROM a GROUP BY a.id",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("active", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("active", DataType::Uuid)]
    )]
    // Case 5: distinct_on - DISTINCT ON clause
    #[case::distinct_on(
        "SELECT DISTINCT ON (a.category) a.  FROM a ORDER BY a.category, a.id",
        35,
        vec![("a", vec![("id", DataType::Uuid), ("category", DataType::Text(None)), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("category", DataType::Text(None)), ("name", DataType::Text(None))]
    )]
    // Case 6: fetch_first_rows - FETCH FIRST syntax (SQL standard)
    #[case::fetch_first_rows(
        "SELECT a.  FROM a ORDER BY a.id FETCH FIRST 10 ROWS ONLY",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 7: grouping_sets - GROUPING SETS in GROUP BY
    #[case::grouping_sets(
        "SELECT a.  FROM a GROUP BY GROUPING SETS ((a.x), (a.y), ())",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid)]
    )]
    // Case 8: cube_rollup - CUBE in GROUP BY
    #[case::cube_rollup(
        "SELECT a.  FROM a GROUP BY CUBE (a.x, a.y)",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid)]
    )]
    // Case 9: coalesce_multiple_args - COALESCE with multiple qualified args
    #[case::coalesce_multiple_args(
        "SELECT COALESCE(a. , a.fallback, 'default') FROM a",
        18,
        vec![("a", vec![("id", DataType::Uuid), ("primary", DataType::Text(None)), ("fallback", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("primary", DataType::Text(None)), ("fallback", DataType::Text(None))]
    )]
    // Case 10: nullif_function - NULLIF function
    #[case::nullif_function(
        "SELECT NULLIF(a. , '') FROM a",
        14,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 11: greatest_least - GREATEST/LEAST functions
    #[case::greatest_least(
        "SELECT GREATEST(a. , a.y, a.z) FROM a",
        17,
        vec![("a", vec![("x", DataType::Uuid), ("y", DataType::Uuid), ("z", DataType::Uuid)])],
        vec![("x", DataType::Uuid), ("y", DataType::Uuid), ("z", DataType::Uuid)]
    )]
    // Case 12: not_in_subquery - NOT IN with subquery
    #[case::not_in_subquery(
        "SELECT a.  FROM a WHERE a.id NOT IN (SELECT b.a_id FROM b)",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("a_id", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 13: any_all_operators - ANY/ALL with array
    #[case::any_all_operators(
        "SELECT a.  FROM a WHERE a.id = ANY(ARRAY[1,2,3])",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 14: is_distinct_from - IS DISTINCT FROM operator
    #[case::is_distinct_from(
        "SELECT a.  FROM a WHERE a.status IS DISTINCT FROM 'active'",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("status", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("status", DataType::Text(None))]
    )]
    // Case 15: similar_to - SIMILAR TO pattern matching
    #[case::similar_to(
        "SELECT a.  FROM a WHERE a.name SIMILAR TO '%(test|prod)%'",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 16: ilike_operator - ILIKE case-insensitive matching
    #[case::ilike_operator(
        "SELECT a.  FROM a WHERE a.name ILIKE '%TEST%'",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 17: overlaps_operator - OVERLAPS for ranges
    #[case::overlaps_operator(
        "SELECT a.  FROM a WHERE (a.start_date, a.end_date) OVERLAPS (CURRENT_DATE, CURRENT_DATE + 7)",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("start_date", DataType::Uuid), ("end_date", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("start_date", DataType::Uuid), ("end_date", DataType::Uuid)]
    )]
    // Case 18: schema_qualified_table - Schema-qualified table reference works
    #[case::schema_qualified_table(
        "SELECT  FROM public.users",
        7,
        vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 19: tablesample_clause - TABLESAMPLE clause is handled
    #[case::tablesample_clause(
        "SELECT a.  FROM a TABLESAMPLE BERNOULLI(10)",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 20: array_constructor - ARRAY constructor in expression
    #[case::array_constructor(
        "SELECT a.  FROM a WHERE a.id = ANY(ARRAY[1, 2, 3]::int[])",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 21: nested_function_calls - Nested function calls with qualified columns
    #[case::nested_function_calls(
        "SELECT UPPER(TRIM(a. )) FROM a",
        19,
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 22: multiple_casts - Multiple type casts in expression
    #[case::multiple_casts(
        "SELECT a. ::text::varchar(100) FROM a",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("value", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("value", DataType::Uuid)]
    )]
    // Case 23: boolean_operators - Boolean operators in WHERE
    #[case::boolean_operators(
        "SELECT a.  FROM a WHERE a.active AND NOT a.deleted OR a.archived",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("active", DataType::Uuid), ("deleted", DataType::Uuid), ("archived", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("active", DataType::Uuid), ("deleted", DataType::Uuid), ("archived", DataType::Uuid)]
    )]
    // Case 24: at_time_zone - AT TIME ZONE expression
    #[case::at_time_zone(
        "SELECT a.  FROM a WHERE a.created_at AT TIME ZONE 'UTC' > NOW()",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("created_at", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("created_at", DataType::Uuid)]
    )]
    // Case 25: interval_expression - INTERVAL literal in expression
    #[case::interval_expression(
        "SELECT a.  FROM a WHERE a.expires_at > NOW() + INTERVAL '1 day'",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("expires_at", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("expires_at", DataType::Uuid)]
    )]
    // Case 26: row_comparison - Row comparison
    #[case::row_comparison(
        "SELECT a.  FROM a WHERE (a.x, a.y) = (1, 2)",
        9,
        vec![("a", vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid)])],
        vec![("id", DataType::Uuid), ("x", DataType::Uuid), ("y", DataType::Uuid)]
    )]
    // Case 27: subquery_in_select_expression - Subquery as part of expression
    #[case::subquery_in_select_expression(
        "SELECT a.  + (SELECT MAX(b.val) FROM b) FROM a",
        9,
        vec![
            ("a", vec![("id", DataType::Uuid), ("amount", DataType::Uuid)]),
            ("b", vec![("val", DataType::Uuid)])
        ],
        vec![("id", DataType::Uuid), ("amount", DataType::Uuid)]
    )]
    #[tokio::test]
    async fn should_handle_postgres_edge_case(
        #[case] sql: &str,
        #[case] cursor_pos: usize,
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = db(&tables).await;
        let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
            .await
            .expect("postgres edge case");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
            .collect();

        assert_eq!(result, expected_columns, "postgres edge case mismatch");
    }
}

#[cfg(test)]
mod dml_statements {
    //! Tests for INSERT, UPDATE, DELETE statement support.
    //!
    //! These tests verify column suggestions work correctly for DML statements,
    //! including the RETURNING clause, SET clause, column lists, and WHERE clauses.

    use super::*;

    mod insert_statements {
        use super::*;

        #[rstest]
        // Case 1: insert_returning_basic - Basic INSERT with RETURNING clause
        #[case::insert_returning_basic(
            "INSERT INTO users (name) VALUES ('test') RETURNING ",
            50,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 2: insert_returning_partial - RETURNING with partial column
        #[case::insert_returning_partial(
            "INSERT INTO orders (product_id, quantity) VALUES (1, 5) RETURNING i",
            67,
            vec![("orders", vec![("id", DataType::Uuid), ("product_id", DataType::Uuid), ("quantity", DataType::Uuid), ("created_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("product_id", DataType::Uuid), ("quantity", DataType::Uuid), ("created_at", DataType::Text(None))]
        )]
        // Case 3: insert_returning_multiple - RETURNING multiple columns
        #[case::insert_returning_multiple(
            "INSERT INTO users (name, email) VALUES ('a', 'b') RETURNING id, ",
            64,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 4: insert_column_list - Cursor in column list
        #[case::insert_column_list(
            "INSERT INTO users (id, ) VALUES (1, 'test')",
            23,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 5: insert_column_list_start - Cursor at start of column list
        #[case::insert_column_list_start(
            "INSERT INTO users ( ) VALUES (1)",
            19,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 6: insert_schema_qualified_table - Schema qualified table name
        #[case::insert_schema_qualified_table(
            "INSERT INTO public.users (name) VALUES ('test') RETURNING ",
            58,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 7: insert_returning_star_position - RETURNING * position
        #[case::insert_returning_star_position(
            "INSERT INTO users DEFAULT VALUES RETURNING ",
            43,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 8: insert_default_values - INSERT with DEFAULT VALUES
        #[case::insert_default_values(
            "INSERT INTO users DEFAULT VALUES RETURNING ",
            43,
            vec![("users", vec![("id", DataType::Uuid), ("created_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("created_at", DataType::Text(None))]
        )]
        // Case 9: insert_multiple_value_rows - Multiple value rows
        #[case::insert_multiple_value_rows(
            "INSERT INTO users (name) VALUES ('a'), ('b'), ('c') RETURNING ",
            62,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 10: insert_on_conflict - INSERT ON CONFLICT with RETURNING
        #[case::insert_on_conflict(
            "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = 'new' RETURNING ",
            102,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        #[tokio::test]
        async fn should_suggest_insert_columns(
            #[case] sql: &str,
            #[case] cursor_pos: usize,
            #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
            #[case] expected: Vec<(&str, DataType)>,
        ) {
            let meta = db(&tables).await;
            let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
                .await
                .expect("INSERT statement test");

            let expected_columns: Vec<_> = expected
                .into_iter()
                .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
                .collect();

            assert_eq!(
                result, expected_columns,
                "INSERT statement columns mismatch"
            );
        }
    }

    mod update_statements {
        use super::*;

        #[rstest]
        // Case 1: update_set_basic - Basic UPDATE SET clause
        #[case::update_set_basic(
            "UPDATE users SET ",
            17,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 2: update_set_multiple - UPDATE SET with multiple columns
        #[case::update_set_multiple(
            "UPDATE users SET name = 'new', ",
            31,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 3: update_returning_basic - UPDATE with RETURNING clause
        #[case::update_returning_basic(
            "UPDATE users SET name = 'new' RETURNING ",
            40,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 4: update_returning_after_where - RETURNING after WHERE
        #[case::update_returning_after_where(
            "UPDATE users SET name = 'new' WHERE id = 1 RETURNING ",
            53,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("updated_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("updated_at", DataType::Text(None))]
        )]
        // Case 5: update_schema_qualified - Schema qualified table
        #[case::update_schema_qualified(
            "UPDATE public.users SET ",
            24,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 6: update_only_keyword - UPDATE ONLY table
        #[case::update_only_keyword(
            "UPDATE ONLY users SET ",
            22,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 7: update_set_expression - SET with expression
        #[case::update_set_expression(
            "UPDATE counters SET count = count + 1, ",
            39,
            vec![("counters", vec![("id", DataType::Uuid), ("count", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("count", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 8: update_set_subquery - SET with subquery value
        #[case::update_set_subquery(
            "UPDATE users SET score = (SELECT MAX(score) FROM scores), ",
            58,
            vec![("users", vec![("id", DataType::Uuid), ("score", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("score", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 9: update_returning_multiple - RETURNING multiple columns
        #[case::update_returning_multiple(
            "UPDATE products SET price = 9.99 RETURNING id, name, ",
            53,
            vec![("products", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("price", DataType::Uuid), ("updated_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("price", DataType::Uuid), ("updated_at", DataType::Text(None))]
        )]
        // Case 10: update_set_default - SET column = DEFAULT
        #[case::update_set_default(
            "UPDATE users SET created_at = DEFAULT, ",
            40,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("created_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("created_at", DataType::Text(None))]
        )]
        #[tokio::test]
        async fn should_suggest_update_columns(
            #[case] sql: &str,
            #[case] cursor_pos: usize,
            #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
            #[case] expected: Vec<(&str, DataType)>,
        ) {
            let meta = db(&tables).await;
            let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
                .await
                .expect("UPDATE statement test");

            let expected_columns: Vec<_> = expected
                .into_iter()
                .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
                .collect();

            assert_eq!(
                result, expected_columns,
                "UPDATE statement columns mismatch"
            );
        }
    }

    mod delete_statements {
        use super::*;

        #[rstest]
        // Case 1: delete_returning_basic - Basic DELETE with RETURNING
        #[case::delete_returning_basic(
            "DELETE FROM users RETURNING ",
            28,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 2: delete_returning_after_where - RETURNING after WHERE
        #[case::delete_returning_after_where(
            "DELETE FROM users WHERE id = 1 RETURNING ",
            41,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 3: delete_returning_multiple - RETURNING multiple columns
        #[case::delete_returning_multiple(
            "DELETE FROM orders WHERE status = 'cancelled' RETURNING id, customer_id, ",
            74,
            vec![("orders", vec![("id", DataType::Uuid), ("customer_id", DataType::Uuid), ("status", DataType::Text(None)), ("total", DataType::Uuid)])],
            vec![("id", DataType::Uuid), ("customer_id", DataType::Uuid), ("status", DataType::Text(None)), ("total", DataType::Uuid)]
        )]
        // Case 4: delete_schema_qualified - Schema qualified table
        #[case::delete_schema_qualified(
            "DELETE FROM public.users RETURNING ",
            35,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 5: delete_only_keyword - DELETE FROM ONLY table
        #[case::delete_only_keyword(
            "DELETE FROM ONLY users RETURNING ",
            33,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 6: delete_without_from - DELETE table (no FROM keyword)
        #[case::delete_without_from(
            "DELETE users RETURNING ",
            23,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 7: delete_returning_star_position - Before * in RETURNING
        #[case::delete_returning_star_position(
            "DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days' RETURNING ",
            74,
            vec![("logs", vec![("id", DataType::Uuid), ("message", DataType::Text(None)), ("created_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("message", DataType::Text(None)), ("created_at", DataType::Text(None))]
        )]
        // Case 8: delete_using_clause - DELETE with USING clause RETURNING
        #[case::delete_using_clause(
            "DELETE FROM orders USING customers WHERE orders.customer_id = customers.id RETURNING ",
            85,
            vec![
                ("orders", vec![("id", DataType::Uuid), ("customer_id", DataType::Uuid)]),
                ("customers", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
            ],
            vec![("id", DataType::Uuid), ("customer_id", DataType::Uuid)]
        )]
        #[tokio::test]
        async fn should_suggest_delete_columns(
            #[case] sql: &str,
            #[case] cursor_pos: usize,
            #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
            #[case] expected: Vec<(&str, DataType)>,
        ) {
            let meta = db(&tables).await;
            let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
                .await
                .expect("DELETE statement test");

            let expected_columns: Vec<_> = expected
                .into_iter()
                .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
                .collect();

            assert_eq!(
                result, expected_columns,
                "DELETE statement columns mismatch"
            );
        }
    }

    mod dml_edge_cases {
        //! Edge cases and complex scenarios for DML statements.

        use super::*;

        #[rstest]
        // Case 1: insert_select_returning - INSERT ... SELECT ... RETURNING
        #[case::insert_select_returning(
            "INSERT INTO archive SELECT * FROM logs WHERE created_at < NOW() RETURNING ",
            75,
            vec![
                ("archive", vec![("id", DataType::Uuid), ("message", DataType::Text(None))]),
                ("logs", vec![("id", DataType::Uuid), ("message", DataType::Text(None)), ("created_at", DataType::Text(None))])
            ],
            vec![("id", DataType::Uuid), ("message", DataType::Text(None))]
        )]
        // Case 2: insert_cte_returning - INSERT with CTE and RETURNING
        #[case::insert_cte_returning(
            "WITH new_data AS (SELECT 1 AS val) INSERT INTO items (value) SELECT val FROM new_data RETURNING ",
            96,
            vec![("items", vec![("id", DataType::Uuid), ("value", DataType::Uuid)])],
            vec![("id", DataType::Uuid), ("value", DataType::Uuid)]
        )]
        // Case 3: update_from_returning - UPDATE with FROM clause and RETURNING
        #[case::update_from_returning(
            "UPDATE orders SET status = 'shipped' FROM customers WHERE orders.customer_id = customers.id RETURNING ",
            102,
            vec![
                ("orders", vec![("id", DataType::Uuid), ("status", DataType::Text(None)), ("customer_id", DataType::Uuid)]),
                ("customers", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
            ],
            vec![("id", DataType::Uuid), ("status", DataType::Text(None)), ("customer_id", DataType::Uuid)]
        )]
        // Case 4: empty_table - Target table not in metadata
        #[case::empty_table(
            "INSERT INTO unknown_table (col) VALUES (1) RETURNING ",
            53,
            vec![],
            vec![]
        )]
        // Case 5: update_set_row_syntax - UPDATE with ROW syntax
        #[case::update_set_row_syntax(
            "UPDATE users SET (name, email) = ('a', 'b') RETURNING ",
            54,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 6: delete_complex_where_returning - DELETE with complex WHERE
        #[case::delete_complex_where_returning(
            "DELETE FROM items WHERE (status = 'expired' OR quantity = 0) AND category_id IN (1, 2, 3) RETURNING ",
            100,
            vec![("items", vec![("id", DataType::Uuid), ("status", DataType::Text(None)), ("quantity", DataType::Uuid), ("category_id", DataType::Uuid)])],
            vec![("id", DataType::Uuid), ("status", DataType::Text(None)), ("quantity", DataType::Uuid), ("category_id", DataType::Uuid)]
        )]
        // Case 7: insert_overriding_system_value - INSERT OVERRIDING SYSTEM VALUE
        #[case::insert_overriding_system_value(
            "INSERT INTO users OVERRIDING SYSTEM VALUE VALUES (1, 'test') RETURNING ",
            71,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 8: update_where_current_of - UPDATE WHERE CURRENT OF cursor
        #[case::update_where_current_of(
            "UPDATE users SET name = 'updated' WHERE CURRENT OF my_cursor RETURNING ",
            71,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
        )]
        // Case 9: multiple_statements_second_insert - Second INSERT in multi-statement
        #[case::multiple_statements_second_insert(
            "INSERT INTO logs (msg) VALUES ('first'); INSERT INTO users (name) VALUES ('test') RETURNING ",
            93,
            vec![
                ("logs", vec![("id", DataType::Uuid), ("msg", DataType::Text(None))]),
                ("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))])
            ],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("email", DataType::Text(None))]
        )]
        // Case 10: returning_with_alias - RETURNING with column alias
        #[case::returning_with_alias(
            "INSERT INTO users (name) VALUES ('test') RETURNING id AS user_id, ",
            67,
            vec![("users", vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("created_at", DataType::Text(None))])],
            vec![("id", DataType::Uuid), ("name", DataType::Text(None)), ("created_at", DataType::Text(None))]
        )]
        #[tokio::test]
        async fn should_handle_dml_edge_case(
            #[case] sql: &str,
            #[case] cursor_pos: usize,
            #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
            #[case] expected: Vec<(&str, DataType)>,
        ) {
            let meta = db(&tables).await;
            let result = Suggestion::search(sql, Cursor::new(cursor_pos, None), meta)
                .await
                .expect("DML edge case test");

            let expected_columns: Vec<_> = expected
                .into_iter()
                .map(|(name, dt)| Suggestion::Column(name.to_string(), dt))
                .collect();

            assert_eq!(result, expected_columns, "DML edge case mismatch");
        }
    }
}
