use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::RwLock;

pub type Data<T> = RwLock<HashMap<String, T>>;
pub type MetaData = LazyLock<Data<Database>>;
pub static METADATA: MetaData = LazyLock::new(|| Data::new(HashMap::new()));

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Default, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Data<Column>,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Data::new(HashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub tables: Data<Table>,
}

impl Schema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tables: Data::new(HashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub schemas: Data<Schema>,
}

impl Database {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schemas: Data::new(HashMap::new()),
        }
    }

    /// Add (or create) schema/table and insert the column.
    pub async fn insert_column(&mut self, schema_name: String, table_name: String, column: Column) {
        let mut schemas = self.schemas.write().await;
        schemas
            .entry(schema_name.clone())
            .or_insert_with(|| Schema::new(&schema_name)) // Create/return schema
            .tables
            .write()
            .await
            .entry(table_name.clone())
            .or_insert_with(|| Table::new(table_name.clone())) // Create/return table
            .columns
            .write()
            .await
            .insert(column.name.clone(), column); // Insert / overwrite column
    }

    /// Add (or create) schema and insert the table.
    pub async fn insert_table(&mut self, schema_name: String, table: Table) {
        let mut schemas = self.schemas.write().await;
        schemas
            .entry(schema_name.clone())
            .or_insert_with(|| Schema::new(&schema_name)) // Create/return schema
            .tables
            .write()
            .await
            .insert(table.name.clone(), table); // Insert / overwrite table
    }

    /// Insert (or overwrite) a schema.
    pub async fn insert_schema(&mut self, schema: Schema) {
        self.schemas
            .write()
            .await
            .insert(schema.name.clone(), schema);
    }
}
