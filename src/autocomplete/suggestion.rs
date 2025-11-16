use crate::*;
use sqlparser::ast::{DataType, Expr, Query, SelectItem, SetExpr, Statement, TableFactor};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Suggestion {
    Keyword(String),
    Column(String, DataType),
    Table(String),
}
pub type Suggestions = Vec<Suggestion>;

impl Suggestion {
    pub async fn suggest(
        statements: Vec<Statement>,
        _cursor: Cursor,
        meta: Database,
    ) -> Result<Suggestions> {
        // (Imports moved to file header)

        // 1. Collect table names from all query statements.
        let mut tables = Vec::new();
        for stmt in statements {
            if let Statement::Query(q) = stmt {
                Self::collect_query(q.as_ref(), &mut tables);
            }
        }
        if tables.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Emit columns preserving metadata insertion order.
        let schemas = meta.schemas.read().await;
        let mut out = Vec::new();
        for schema in schemas.values() {
            let tab_map = schema.tables.read().await;
            for t in &tables {
                if let Some(tab) = tab_map.get(t) {
                    for (n, dt) in tab.ordered_columns().await {
                        out.push(Suggestion::Column(n, dt));
                    }
                }
            }
        }
        Ok(out)
    }

    fn last_ident(tf: &TableFactor) -> Option<String> {
        if let TableFactor::Table { name, .. } = tf {
            name.0
                .last()
                .and_then(|p| p.as_ident())
                .map(|i| i.value.clone())
        } else {
            None
        }
    }

    fn collect_expr(e: &Expr, out: &mut Vec<String>) {
        match e {
            Expr::Subquery(q) => Self::collect_query(q, out),
            Expr::Nested(inner) => Self::collect_expr(inner, out),
            _ => {}
        }
    }

    fn collect_query(q: &Query, out: &mut Vec<String>) {
        if let SetExpr::Select(sel) = &*q.body {
            // FROM and JOIN tables
            for from in &sel.from {
                if let Some(t) = Self::last_ident(&from.relation) {
                    out.push(t);
                }
                for j in &from.joins {
                    if let Some(t) = Self::last_ident(&j.relation) {
                        out.push(t);
                    }
                }
            }
            // Projection expressions (for nested subqueries)
            for item in &sel.projection {
                match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        Self::collect_expr(e, out)
                    }
                    _ => {}
                }
            }
        }
    }
}

// Backward-compatible free function (tests call this).
pub async fn suggest(
    statements: Vec<Statement>,
    cursor: Cursor,
    meta: Database,
) -> Result<Suggestions> {
    Suggestion::suggest(statements, cursor, meta).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use sqlparser::ast::DataType;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    static POSTGRES: PostgreSqlDialect = PostgreSqlDialect {};

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

    #[rstest]
    #[case("SELECT  FROM example", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    #[case("SELECT  FROM example", (7, None), vec![("example", vec![("id", DataType::Uuid), ("name", DataType::Text)])])]
    #[case("SELECT (SELECT  FROM example) FROM other", (15, None), vec![("example", vec![("id", DataType::Uuid), ("name", DataType::Text)])])]
    #[case("SELECT  FROM example, users", (7, None), vec![("example", vec![("id", DataType::Uuid)]), ("users", vec![("user_id", DataType::Uuid), ("email", DataType::Text)])])]
    #[case("SELECT  FROM example JOIN users ON example.id = users.example_id", (7, None), vec![("example", vec![("id", DataType::Uuid)]), ("users", vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)])])]
    // Alias (simple)
    #[case("SELECT  FROM example e", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // Alias with AS
    #[case("SELECT  FROM example AS ex", (7, None), vec![("example", vec![("id", DataType::Uuid)])])]
    // No FROM clause should yield no suggestions
    #[case("SELECT 1", (7, None), vec![])]
    // Deeply nested subquery
    #[case("SELECT (SELECT (SELECT  FROM inner)) FROM outer", (22, None), vec![("inner", vec![("iid", DataType::Uuid)])])]
    // Same column names across two tables
    #[case("SELECT  FROM a, b", (7, None), vec![
        ("a", vec![("id", DataType::Uuid), ("name", DataType::Text)]),
        ("b", vec![("id", DataType::Uuid), ("name", DataType::Text)]),
    ])]
    // Join with aliases
    #[case("SELECT  FROM a AS x JOIN b AS y ON x.id = y.id", (7, None), vec![
        ("a", vec![("id", DataType::Uuid), ("name", DataType::Text)]),
        ("b", vec![("id", DataType::Uuid), ("name", DataType::Text)]),
    ])]
    #[tokio::test]
    async fn should_recommend_columns(
        #[case] sql: &str,
        #[case] (start, end): (usize, Option<usize>),
        #[case] tables: Vec<(&str, Vec<(&str, DataType)>)>,
    ) {
        // When
        let meta = database("postgres", &tables).await;
        let statements = Parser::parse_sql(&POSTGRES, sql).expect("parse");

        // Then
        let result = suggest(statements, Cursor::new(start, end), meta)
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

    #[rstest]
    // Suggestions for users table
    #[case(
        "SELECT users.  FROM example JOIN users ON example.id = users.example_id",
        (13, None),
        vec![
            ("example", vec![("id", DataType::Uuid)]),
            ("users", vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)])
        ],
        vec![("user_id", DataType::Uuid), ("example_id", DataType::Uuid)]
    )]
    /// Ssuggestions for table 'a' when both tables have identical column names
    #[case(
        "SELECT a.  FROM a JOIN b ON a.id = b.id",
        (9, None),
        vec![
            ("a", vec![("id", DataType::Uuid), ("name", DataType::Text)]),
            ("b", vec![("id", DataType::Uuid), ("name", DataType::Text)])
        ],
        vec![("id", DataType::Uuid), ("name", DataType::Text)]
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
        let statements = Parser::parse_sql(&POSTGRES, sql).expect("parse");

        // Then
        let result = suggest(statements, Cursor::new(start, end), meta)
            .await
            .expect("suggestion shouldnt error");

        // Should
        let expected_columns: Vec<_> = expected
            .into_iter()
            .map(|(name, data_type)| Suggestion::Column(name.to_string(), data_type))
            .collect();

        // Desired behavior: only columns belonging to the qualified table prefix.
        assert_eq!(
            result, expected_columns,
            "qualified suggestions should only include columns from the referenced table prefix"
        );
    }
}
