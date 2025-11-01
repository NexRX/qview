//! Validator module for parsing and validating SQL queries.
use crate::*;
use sqlx::{Executor as _, PgPool, SqlStr, postgres::PgStatement};

pub struct Validator {
    pool: PgPool,
}

impl Validator {
    pub async fn sql(&self, sql: impl Into<SqlStr>) -> Result<PgStatement> {
        self.pool.prepare(sql.into()).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{
        Column as _, Statement as _,
        postgres::PgErrorPosition::{self, *},
    };

    #[test_context(IsolatedIntegrationTest)]
    #[rstest]
    #[case("SELECT 1", &["?column?"])]
    #[case("SELECT 1 as one", &["one"])]
    #[case("SELECT table_name FROM information_schema.tables", &["table_name"])]
    #[tokio::test]
    pub async fn when_valid_parameterless_query_then_success(
        ctx: &mut IsolatedIntegrationTest,
        #[case] sql: &'static str,
        #[case] columns: &[&'static str],
    ) {
        let validate = Validator {
            pool: ctx.pool.clone(),
        };
        let result = validate.sql(SqlStr::from_static(sql)).await;
        assert!(result.is_ok(), "Expected Ok(PgStatement), got {result:?}");

        let statement = result.unwrap();
        assert_eq!(statement.columns().len(), columns.len());
        let actual_columns = statement
            .columns()
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>();
        assert_eq!(actual_columns, columns);
    }

    #[test_context(IsolatedIntegrationTest)]
    #[rstest]
    #[case("SELECT 1!", "42601", "syntax error at end of input", Original(10))]
    #[case("!SELECT 1", "42601", r#"syntax error at or near "!""#, Original(1))]
    #[case(
        "SELECT * TABLE;",
        "42601",
        r#"syntax error at or near "TABLE""#,
        Original(10)
    )]
    #[case(
        "SELECT col1, col2 TABLE;",
        "42703",
        r#"column "col1" does not exist"#,
        Original(8)
    )]
    #[tokio::test]
    pub async fn when_invalid<'a>(
        ctx: &mut IsolatedIntegrationTest,
        #[case] sql: &'static str,
        #[case] code: &'static str,
        #[case] message: &'static str,
        #[case] position: PgErrorPosition<'a>,
    ) {
        use sqlx::postgres::PgDatabaseError;

        let validate = Validator {
            pool: ctx.pool.clone(),
        };
        let result = validate.sql(SqlStr::from_static(sql)).await;
        assert!(
            result.is_err(),
            "Expected Err(PgErrorPosition), got {result:?}"
        );

        let err = result.unwrap_err();
        match err {
            Error::Database(sqlx::Error::Database(db_error))
                if db_error.try_downcast_ref::<PgDatabaseError>().is_some() =>
            {
                let error = db_error.downcast::<PgDatabaseError>();
                assert_eq!(
                    (error.code(), error.message(), error.position()),
                    (code, message, Some(position))
                );
            }
            err => panic!("Unexpected kind of err {err:?}"),
        }
    }
}
