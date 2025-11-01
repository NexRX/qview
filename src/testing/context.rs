use crate::testing::*;
use sqlx::{PgPool, Postgres};
use test_context::AsyncTestContext;
pub use test_context::test_context;

pub struct IsolatedIntegrationTest {
    pub pool: PgPool,
    pub database: String,
    pub is_teardown: bool,
}

impl IsolatedIntegrationTest {
    async fn random_database<'c, E: sqlx::Executor<'c, Database = Postgres>>(exec: E) -> String {
        use rand::Rng;
        let db = format!(
            "test_db_{}",
            rand::rng()
                .sample_iter(&rand::distr::Alphanumeric)
                .take(8)
                .map(char::from)
                .collect::<String>()
                .to_lowercase()
        );

        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {db}")))
            .execute(exec)
            .await
            .expect("Failed to create test database");
        db
    }
}

impl AsyncTestContext for IsolatedIntegrationTest {
    async fn setup() -> Self {
        crate::testing::common_init();
        let postgres_pool = pool("postgres").await;
        let database = Self::random_database(&postgres_pool).await;

        Self {
            pool: pool(&database).await,
            database,
            is_teardown: true,
        }
    }

    async fn teardown(self) {
        if !self.is_teardown {
            return;
        }

        self.pool.close().await;

        let pool = pool("postgres").await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE {}",
            self.database
        )))
        .execute(&pool)
        .await
        .expect("Failed to drop test database");
    }
}
