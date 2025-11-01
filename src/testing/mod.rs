#![cfg(test)]
crate::reexport!(container);
crate::reexport!(context);
pub use rstest::*;

pub(in crate::testing) fn common_init() {
    use std::sync::Once;
    use tracing_subscriber::EnvFilter;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Only initialize once for all tests
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env()) // <- reads RUST_LOG
            .with_test_writer() // ensures it integrates with `cargo test` output
            .init();
    });
}

mod isolated_integration_tests {
    use super::{super::*, *};

    #[test_context(IsolatedIntegrationTest)]
    #[tokio::test]
    async fn can_connect(ctx: &mut IsolatedIntegrationTest) -> Result {
        sqlx::query("SELECT 1;").fetch_one(&ctx.pool).await?;
        Ok(())
    }

    #[test_context(IsolatedIntegrationTest)]
    #[tokio::test]
    async fn can_read(ctx: &mut IsolatedIntegrationTest) -> Result {
        let database: String = sqlx::query_scalar("SELECT current_database();")
            .fetch_one(&ctx.pool)
            .await?;
        assert_eq!(ctx.database, database);
        Ok(())
    }

    #[test_context(IsolatedIntegrationTest)]
    #[rstest]
    #[case(1, "first_test")]
    #[case(1, "second_test")]
    #[tokio::test]
    async fn can_write(
        ctx: &mut IsolatedIntegrationTest,
        #[case] id: i32,
        #[case] name: &str,
    ) -> Result {
        // Create a basic table
        sqlx::query("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255))")
            .execute(&ctx.pool)
            .await?;

        // Write data to the table
        sqlx::query("INSERT INTO test_table (id, name) VALUES ($1, $2)")
            .bind(id)
            .bind(name)
            .execute(&ctx.pool)
            .await?;

        // Read the data back and assert the write was successful
        let actual_name: String = sqlx::query_scalar("SELECT name FROM test_table WHERE id = $1")
            .bind(id)
            .fetch_one(&ctx.pool)
            .await?;

        assert_eq!(name, actual_name);

        Ok(())
    }
}
