use super::*;
use crate::*;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, Column>,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: BTreeMap::new(),
        }
    }

    pub fn new_with(
        name: impl Into<String>,
        columns: impl Into<BTreeMap<String, DataType>>,
    ) -> Self {
        let columns_map = columns.into();
        Self {
            name: name.into(),
            columns: Column::new_map(columns_map),
        }
    }

    /// Construct a table with an explicit ordered list of (name, DataType) pairs.
    /// This preserves the ordering exactly as provided.
    pub fn new_with_ordered(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = (impl Into<String>, DataType)>,
    ) -> Self {
        let mut map = BTreeMap::new();
        let mut order = Vec::new();
        for (n, dt) in columns.into_iter() {
            let name_str = n.into();
            order.push(name_str.clone());
            map.insert(name_str.clone(), dt);
        }
        Self {
            name: name.into(),
            columns: Column::new_map(map),
        }
    }

    pub async fn from_postgres(conn: &mut PgConnection, table: &str) -> sqlx::Result<Self> {
        let columns: BTreeMap<String, DataType> = sqlx::query!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
            table
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .filter_map(|row| match (row.column_name, row.data_type) {
            (Some(name), Some(data_type)) => Some((name, data_type)),
            _ => None,
        })
        .map(|(name, data_type)| (name, DataType::from(data_type)))
        .collect();

        Ok(Self::new_with(table, columns))
    }
}
