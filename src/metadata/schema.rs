use itertools::Itertools as _;

use super::*;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub tables: Data<Table>,
}

impl Schema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tables: BTreeMap::new(),
        }
    }

    /// Insert (or overwrite) a table.
    pub fn insert_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    #[cfg(feature = "backend-impl")]
    pub async fn from_postgres(conn: &mut sqlx::PgConnection, schema: &str) -> sqlx::Result<Self> {
        let mut metadata = Self::new(schema.to_string());

        let tables: Vec<String> = sqlx::query_scalar!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
            schema
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .flatten()
        .collect_vec();

        for table in tables {
            metadata.insert_table(Table::from_postgres(conn, &table).await?);
        }

        Ok(metadata)
    }
}
