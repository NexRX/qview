use itertools::Itertools;

use super::*;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub schemas: Data<Schema>,
}

impl Database {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schemas: BTreeMap::new(),
        }
    }

    /// Add (or create) schema/table and insert the column.
    pub fn insert_column(&mut self, schema_name: String, table_name: String, column: Column) {
        self.schemas
            .entry(schema_name.clone())
            .or_insert_with(|| Schema::new(&schema_name)) // Create/return schema
            .tables
            .entry(table_name.clone())
            .or_insert_with(|| Table::new(table_name.clone())) // Create/return table
            .columns
            .insert(column.name.clone(), column); // Insert / overwrite column
    }

    /// Add (or create) schema and insert the table.
    pub fn insert_table(&mut self, schema_name: impl Display, table: Table) {
        self.schemas
            .entry(schema_name.to_string())
            .or_insert_with(|| Schema::new(schema_name.to_string())) // Create/return schema
            .tables
            .insert(table.name.clone(), table); // Insert / overwrite table
    }

    /// Insert (or overwrite) a schema.
    pub fn insert_schema(&mut self, schema: Schema) {
        self.schemas.insert(schema.name.clone(), schema);
    }

    #[cfg(feature = "backend-impl")]
    pub async fn from_postgres(
        conn: &mut sqlx::PgConnection,
        database: &str,
    ) -> sqlx::Result<Database> {
        let mut metadata = Self::new(database.to_string());

        let schemas: Vec<String> = sqlx::query_scalar!(
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = $1",
            database
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .flatten()
        .collect_vec();

        for schema in schemas {
            metadata.insert_schema(Schema::from_postgres(conn, &schema).await?);
        }

        Ok(metadata)
    }
}
