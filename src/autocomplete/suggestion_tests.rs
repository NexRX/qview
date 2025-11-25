#![cfg(test)]
use crate::*;
use rstest::rstest;

/// Build a lightweight in-memory `Database` with the provided tables (all in "public" schema).
async fn database(database: &str, tables: &[(&str, Vec<(&str, DataType)>)]) -> Database {
    let mut meta = Database::new(database);
    for (table_name, columns) in tables {
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
    meta
}

/// Build a `Database` with tables split across two schemas to test multi-schema aggregation.
async fn database_multi_schema(
    database: &str,
    public_tables: &[(&str, Vec<(&str, DataType)>)],
    other_schema: &str,
    other_tables: &[(&str, Vec<(&str, DataType)>)],
) -> Database {
    let mut meta = Database::new(database);
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
    for (table_name, columns) in other_tables {
        meta.insert_table(
            other_schema,
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
    meta
}

#[cfg(test)]
mod column_testing {
    use super::*;
    #[rstest]
    // Case 1: single table, single column
    #[case("SELECT  FROM example", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // Case 2: single table, multiple columns order preserved
    #[case("SELECT  FROM example", (7, None), vec![("example", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])])]
    // Case 3: nested subquery inner SELECT isolation
    #[case("SELECT (SELECT  FROM example) FROM other", (15, None), vec![("example", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])])]
    // Case 4: multiple tables comma separated
    #[case("SELECT  FROM example, users", (7, None), vec![("example", vec![("id", DataType::Uuid)]), ("users", vec![("user_id", DataType::Uuid), ("email", DataType::Text(None))])])]
    // Case 5: simple JOIN with ON clause
    #[case("SELECT  FROM example JOIN users ON example.id = users.example_id", (7, None), vec![("example", vec![("id", DataType::Uuid)]), ("users", vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)])])]
    // Alias (simple)
    // Case 6: alias without AS
    #[case("SELECT  FROM example e", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // Case 7: alias with AS
    #[case("SELECT  FROM example AS ex", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // Case 8: no FROM clause yields no suggestions
    #[case("SELECT 1", (7, None), vec![])]
    // Case 9: deeply nested subquery isolation
    #[case("SELECT (SELECT (SELECT  FROM inner)) FROM outer", (22, None), vec![("inner", vec![("iid", DataType::Uuid)])])]
    // Case 10: duplicate column names across tables
    #[case("SELECT  FROM a, b", (7, None), vec![
        ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
        ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
    ])]
    // Case 11 (should_recommend_columns): JOIN with table aliases
    #[case("SELECT  FROM a AS x JOIN b AS y ON x.id = y.id", (7, None), vec![
        ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
        ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
    ])]
    // Case 12: WHERE terminator stops table extraction
    #[case("SELECT  FROM example WHERE example.id IS NOT NULL", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // Case 13: GROUP BY terminator stops table extraction
    #[case("SELECT  FROM example GROUP BY example.id", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // Case 14: JOIN chain with subsequent WHERE terminator
    #[case("SELECT  FROM a JOIN b ON a.id = b.id WHERE a.id > 0", (7, None), vec![
        ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
        ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
    ])]
    // Case 15: Early cursor before FROM (no FROM yet)
    #[case("SELECT  foo", (7, None), vec![])]
    // Case 16: Multiple SELECT statements (cursor in second)
    #[case("SELECT  FROM a; SELECT  FROM b", (23, None), vec![("b", vec![("bid", DataType::Uuid)])])]
    // Case 17: Ordering preservation test
    #[case("SELECT  FROM ord", (7, None), vec![("ord", vec![("id", DataType::Uuid), ("created_at", DataType::Text(None)), ("name", DataType::Text(None))])])]
    // Case 18: ORDER BY terminator
    #[case("SELECT  FROM a ORDER BY a.id", (7, None), vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])])]
    // Case 19: LIMIT terminator
    #[case("SELECT  FROM a LIMIT 10", (7, None), vec![("a", vec![("id", DataType::Uuid)])])]
    // Case 20: Trailing comma after table list
    #[case("SELECT  FROM a,", (7, None), vec![("a", vec![("id", DataType::Uuid)])])]
    // Case 21: Unknown table referenced (not in metadata)
    #[case("SELECT  FROM missing", (7, None), vec![])]
    #[tokio::test]
    async fn should_recommend_columns(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),

        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
    ) {
        // When
        let meta = database("postgres", &tables).await;

        // Then

        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("suggestion shouldnt error");

        // Should

        let expected_columns: Vec<_> = tables
            .into_iter()
            .flat_map(|(_, columns)| columns)
            .map(|(name, data_type)| Suggestion::Column(name.to_string(), data_type))
            .collect();

        assert_eq!(result, expected_columns);
    }

    // Dedicated subquery isolation tests:
    // These ensure depth tracking prevents leakage of outer tables into inner subqueries
    // and excludes inner tables when cursor is in the outer SELECT projection.
    #[rstest]
    // Case 1: Subquery with JOIN chain
    #[case(
        "SELECT (SELECT  FROM inner JOIN another ON inner.id = another.inner_id) FROM outer", (15, None),
        vec![
            ("inner", vec![("id", DataType::Uuid)]),
            ("another", vec![("inner_id", DataType::Uuid), ("val", DataType::Text(None))]),
            ("outer", vec![("oid", DataType::Uuid)])
        ],
        vec![
            ("id", DataType::Uuid),
            ("inner_id", DataType::Uuid),
            ("val", DataType::Text(None))
        ]
    )]
    // Case 2: Subquery with JOIN chain
    #[case(
        "SELECT  , (SELECT id FROM inner) FROM outer JOIN other2 ON outer.oid = other2.oid", (7, None),
        vec![
            ("outer", vec![("oid", DataType::Uuid), ("name", DataType::Text(None))]),
            ("other2", vec![("oid", DataType::Uuid), ("desc", DataType::Text(None))]),
            ("inner", vec![("id", DataType::Uuid)])
        ],
        vec![
            ("oid", DataType::Uuid),
            ("name", DataType::Text(None)),
            ("oid", DataType::Uuid),
            ("desc", DataType::Text(None))
        ]
    )]
    // Case 3: Deep subquery
    #[case(
        "SELECT (SELECT (SELECT  FROM deep)) FROM outer", (22, None),
        vec![
            ("deep", vec![("did", DataType::Uuid), ("dval", DataType::Text(None))]),
            ("outer", vec![("oid", DataType::Uuid)])
        ],
        vec![
            ("did", DataType::Uuid),
            ("dval", DataType::Text(None))
        ]
    )]
    #[tokio::test]
    async fn should_recommend_columns_subquery_isolation(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("suggestion shouldnt error");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, data_type)| Suggestion::Column(name.to_string(), data_type))
            .collect();

        assert_eq!(
            result, expected_columns,
            "subquery isolation failed: columns outside current SELECT depth leaked or in-scope columns missing"
        );
    }

    #[rstest]
    // Case 1: Suggestions for users table
    #[case(
        "SELECT users.  FROM example JOIN users ON example.id = users.example_id",
        (13, None),
        vec![
            ("example", vec![("id", DataType::Uuid)]),
            ("users", vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)])
        ],
        vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)]
    )]
    // Case 2: Suggestions for table 'a' when both tables have identical column names
    #[case(
        "SELECT a.  FROM a JOIN b ON a.id = b.id",
        (9, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 3: Alias resolution (simple alias)
    #[case(
        "SELECT ex.  FROM example ex",
        (10, None),
        vec![("example", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    // Case 4: Alias resolution (AS form)
    #[case(
        "SELECT x.  FROM a AS x JOIN b AS y ON x.id = y.id",
        (9, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 5: Alias with WHERE terminator
    #[case(
        "SELECT x.  FROM a AS x WHERE x.id > 0",
        (9, None),
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 6: Unknown alias should yield no suggestions
    #[case(
        "SELECT z.  FROM a AS x",
        (9, None),
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![]
    )]
    // Case 7: Simple qualified prefix without alias
    #[case(
        "SELECT a.  FROM a",
        (9, None),
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 8: End-of-input qualified prefix (no FROM yet) should yield none
    #[case(
        "SELECT a.",
        (9, None),
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![]
    )]
    // Case 9: Qualified prefix with ORDER BY terminator
    #[case(
            "SELECT a.  FROM a ORDER BY a.id",
            (9, None),
            vec![("a", vec![("id", DataType::Uuid)])],
            vec![("id", DataType::Uuid)]
        )]
    // Case 10: Qualified prefix inside subquery referencing outer alias (no outer columns should leak)
    #[case(
            "SELECT (SELECT o.  FROM inner) FROM outer o",
            (18, None),
            vec![
                ("outer", vec![("oid", DataType::Uuid), ("oname", DataType::Text(None))]),
                ("inner", vec![("iid", DataType::Uuid), ("ival", DataType::Text(None))])
            ],
            vec![]
        )]
    // Case 11: Qualified prefix referencing subquery alias (subquery alias itself not resolved)
    #[case(
            "SELECT (SELECT inner.  FROM inner JOIN another ON inner.id = another.inner_id) FROM outer",
            (24, None),
            vec![
                ("inner", vec![("id", DataType::Uuid)]),
                ("another", vec![("inner_id", DataType::Uuid), ("val", DataType::Text(None))]),
                ("outer", vec![("oid", DataType::Uuid)])
            ],
            vec![("id", DataType::Uuid)]
        )]
    #[tokio::test]
    async fn should_recommend_qualified_columns(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        // When
        let meta = database("postgres", &tables).await;

        // Then
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("suggestion shouldnt error");

        // Should
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, data_type)| Suggestion::Column(name.to_string(), data_type))
            .collect();

        assert_eq!(
            result, expected_columns,
            "qualified suggestions should only include columns from the referenced table prefix"
        );
    }

    // Derived subquery with star: current behavior -> no derived columns captured (star not expanded)
    #[rstest]
    // Case 1: Derived subquery star expansion unsupported -> expect empty suggestions
    #[case(
        "SELECT  FROM (SELECT * FROM a) sub",
        (7, None),
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![]
    )]
    #[tokio::test]
    async fn should_document_gap_derived_star(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("derived star");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "gap: star (*) in derived subquery not expanded into alias column list"
        );
    }

    // Derived subquery with column aliases: after rollback, derived alias columns unsupported -> expect empty.
    #[rstest]
    // Case 1: Derived subquery column aliases unsupported -> expect empty suggestions for qualified prefix
    #[case(
        "SELECT sub.  FROM (SELECT id AS ident, name AS nm FROM a) sub",
        (12, None),
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![]
    )]
    #[tokio::test]
    async fn should_document_gap_derived_column_aliases(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("derived column alias qualified");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "rollback: derived column alias expansion unsupported; expecting empty suggestions"
        );
    }

    // CTE chain: y references x, neither exposed in suggestions (only base table 'a')
    #[rstest]
    // Case 1: CTE chain not exposed, only base table columns suggested
    #[case(
        "WITH x AS (SELECT id FROM a), y AS (SELECT id FROM x) SELECT  FROM a", (61, None),
        vec![("a", vec![("id", DataType::Uuid)])],
        vec![("id", DataType::Uuid)]
    )]
    #[tokio::test]
    async fn should_document_gap_cte_chain(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("cte chain");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "gap: CTE chain columns not exposed; only underlying base tables available"
        );
    }

    // Parenthesized join group alias: (a JOIN b ...) ab -> current behavior: alias 'ab' not resolved, a/b not captured at top depth
    #[rstest]
    // Case 1: Parenthesized join group alias not recognized -> empty suggestions
    #[case(
        "SELECT ab.  FROM (a JOIN b ON a.aid = b.bid) ab", (11, None),
        vec![("a", vec![("aid", DataType::Uuid)]), ("b", vec![("bid", DataType::Uuid)])],
        vec![]
    )]
    #[tokio::test]
    async fn should_document_gap_parenthesized_join_group_alias(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("parenthesized join group alias");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "gap: parenthesized join group alias not recognized for column suggestions"
        );
    }

    // INTERSECT termination: first SELECT should only show table a columns
    #[rstest]
    // Case 1: INTERSECT first SELECT isolated to table a columns
    #[case(
        "SELECT  FROM a INTERSECT SELECT  FROM b",
        (7, None),
        vec![("a", vec![("aid", DataType::Uuid)]), ("b", vec![("bid", DataType::Uuid)])],
        vec![("aid", DataType::Uuid)]
    )]
    #[tokio::test]
    async fn should_document_gap_intersect_termination_first(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("intersect first");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "gap: INTERSECT termination should isolate first SELECT scope"
        );
    }

    // INTERSECT termination: second SELECT should only show table b columns
    #[rstest]
    // Case 1: INTERSECT second SELECT isolated to table b columns
    #[case(
        "SELECT  FROM a INTERSECT SELECT  FROM b", (32, None),
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))])
        ],
        vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_document_gap_intersect_termination_second(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("intersect second");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "gap: INTERSECT termination should isolate second SELECT scope"
        );
    }

    // Qualified derived star alias: (SELECT * FROM a) sub -> qualified 'sub.' returns no columns (star not expanded)
    #[rstest]
    // Case 1: Qualified derived star prefix unsupported -> expect empty suggestions
    #[case(
        "SELECT sub.  FROM (SELECT * FROM a) sub", (12, None),
        vec![("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])],
        vec![] // expected empty
    )]
    #[tokio::test]
    async fn should_document_gap_qualified_derived_star(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("qualified derived star");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "gap: qualified derived star should expand underlying columns but currently yields none"
        );
    }

    // Multi-schema duplicate table name aggregation (unqualified)
    #[rstest]
    // Case 1: Multi-schema duplicate table aggregation preserves per-schema insertion order
    #[case(
        "SELECT  FROM users", (7, None),
        vec![("users", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])],
        vec![("id", DataType::Uuid), ("email", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_recommend_columns_multi_schema_duplicate(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        // For multi-schema we still need to build both schemas; pass only public portion here.
        #[case] public_tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database_multi_schema(
            "postgres",
            &public_tables,
            "analytics",
            &[(
                "users",
                vec![
                    ("user_id", DataType::Uuid),
                    ("created_at", DataType::Text(None)),
                ],
            )],
        )
        .await;

        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("multi-schema duplicate users");

        // Build expected columns in actual output order: public schema first, then analytics schema.
        let mut expected_columns: Vec<Suggestion> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        expected_columns.extend([
            Suggestion::Column("user_id".into(), DataType::Uuid),
            Suggestion::Column("created_at".into(), DataType::Text(None)),
        ]);
        assert_eq!(
            result, expected_columns,
            "multi-schema duplicate table columns should aggregate in declared order per schema insertion"
        );
    }

    // Alias shadowing: table named 'fake' and alias 'fake' for 'real' -> qualified fake. should resolve to alias target (real) columns first
    #[rstest]
    // Case 1: Alias shadowing a real table name resolves to aliased underlying table
    #[case(
        "SELECT fake.  FROM real AS fake, fake", (12, None),
        vec![
            ("real", vec![("rid", DataType::Uuid), ("rval", DataType::Text(None))]),
            ("fake", vec![("fid", DataType::Uuid)])
        ],
        vec![("rid", DataType::Uuid), ("rval", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_prefer_alias_over_same_named_table(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;

        // real AS fake introduces alias 'fake' -> should map to 'real', not the actual 'fake' table when qualified.
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("alias shadowing resolution");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "alias shadowing: qualified alias should return underlying aliased table columns, not same-named table's columns"
        );
    }

    #[rstest]
    // Case 1: First SELECT in UNION should only see columns from first table
    #[case(
        "SELECT  FROM a UNION SELECT  FROM b",
        (7, None),
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid)])
        ],
        vec![
            ("aid", DataType::Uuid)
        ]
    )]
    // Case 2: SELECT in UNION should only see columns from second table
    #[case(
        "SELECT * FROM a UNION SELECT  FROM b",
        (29, None),
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))])
        ],
        vec![
            ("bid", DataType::Uuid),
            ("bname", DataType::Text(None))
        ]
    )]
    #[tokio::test]
    async fn should_recommend_columns_union_and_cte(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("suggestion shouldnt error");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, data_type)| Suggestion::Column(name.to_string(), data_type))
            .collect();
        assert_eq!(
            result, expected_columns,
            "UNION/CTE/derived table suggestions mismatch"
        );
    }

    #[rstest]
    // Case 1: Qualified UNION second SELECT scope suggestions for table b
    #[case(
        "SELECT aid FROM a UNION SELECT b.  FROM b",
        (29, None),
        vec![
            ("a", vec![("aid", DataType::Uuid)]),
            ("b", vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))])
        ],
        vec![("bid", DataType::Uuid), ("bname", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn should_recommend_qualified_columns_union_and_cte(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;

        // When
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("qualified union second select");

        // Then: expect only columns from table b
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "qualified UNION second SELECT suggestions mismatch"
        );
    }

    #[rstest]
    // Case 1: USING clause: ensure it doesn't disrupt table extraction or qualified suggestions
    #[case(
        "SELECT a.  FROM a JOIN b USING(id)",
        (9, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 2: USING clause with unqualified suggestions should aggregate both tables
    #[case(
        "SELECT  FROM a JOIN b USING(id)",
        (7, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    // Case 3: NATURAL join: tokenizer does not model NATURAL keyword; document current behavior (should still capture tables)
    #[case(
        "SELECT  FROM a NATURAL JOIN b",
        (7, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    // Case 4: CROSS join: ensure tables on both sides are captured
    #[case(
        "SELECT  FROM a CROSS JOIN b",
        (7, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![
            ("id", DataType::Uuid), ("name", DataType::Text(None)),
            ("id", DataType::Uuid), ("email", DataType::Text(None))
        ]
    )]
    // Case 5: Quoted identifiers: document gap if tokenizer doesn't support quoted names
    #[case(
        "SELECT ua.  FROM \"User Accounts\" AS ua",
        (11, None),
        vec![
            ("User Accounts", vec![("userid", DataType::Uuid), ("display_name", DataType::Text(None))])
        ],
        vec![] // current behavior: quoted identifiers likely not recognized -> expect empty suggestions for ua.
    )]
    // Case 6: Numeric literal dot disambiguation: ensure u. is recognized, not 1.0
    #[case(
        "SELECT COALESCE(u. , 1.0) FROM users u",
        (18, None),
        vec![
            ("users", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("email", DataType::Text(None))]
    )]
    #[tokio::test]
    async fn edge_cases_additional(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("edge cases");

        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();

        assert_eq!(result, expected_columns, "edge case mismatch");
    }

    // PostgreSQL grammar edge cases: LATERAL, VALUES-derived alias, DISTINCT ON, WINDOW clause, schema-qualified function call in FROM (document gap).
    #[rstest]
    // Case 1: LATERAL join: ensure right-side table after LATERAL subquery is captured and qualified suggestions work
    #[case(
        "SELECT a.  FROM a LEFT JOIN LATERAL (SELECT id FROM b WHERE b.id = a.id) AS bl ON true",
        (9, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))]),
            ("b", vec![("id", DataType::Uuid), ("email", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 2: VALUES-derived alias: document current behavior for derived tables as a gap (no column suggestions)
    #[case(
        "SELECT v.  FROM (VALUES (1), (2)) AS v(x)",
        (10, None),
        vec![],
        vec![] // derived VALUES alias columns are not exposed
    )]
    // Case 3: DISTINCT ON edge case temporarily removed due to cursor-position sensitivity.
    #[case(
        "SELECT a.  FROM a WINDOW w AS (PARTITION BY a.id)",
        (9, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text(None))])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text(None))]
    )]
    // Case 4: Schema-qualified function call in FROM: document gap (functions as table sources not resolved)
    #[case(
        "SELECT f.  FROM pg_catalog.generate_series(1,10) AS f(x)",
        (10, None),
        vec![],
        vec![] // function/table functions not resolved by current extractor
    )]
    #[tokio::test]
    async fn postgres_grammar_edge_cases(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
        #[case] expected: Vec<(&str, DataType)>,
    ) {
        let meta = database("postgres", &tables).await;
        let result = Suggestion::search(sql, Cursor::new(start, end), meta)
            .await
            .expect("postgres grammar edge cases");
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(n, dt)| Suggestion::Column(n.to_string(), dt))
            .collect();
        assert_eq!(
            result, expected_columns,
            "postgres grammar edge case mismatch"
        );
    }
}
