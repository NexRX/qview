use super::*;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Data<Column>,
    // Preserve insertion order of columns as provided at construction time.
    pub column_order: Vec<String>,
}

impl Default for Table {
    fn default() -> Self {
        Table {
            name: String::new(),
            columns: Data::new(HashMap::new()),
            column_order: Vec::new(),
        }
    }
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Data::new(HashMap::new()),
            column_order: Vec::new(),
        }
    }

    pub fn new_with(
        name: impl Into<String>,
        columns: impl Into<HashMap<String, DataType>>,
    ) -> Self {
        let columns_map = columns.into();
        let order = columns_map.keys().cloned().collect::<Vec<_>>();
        Self {
            name: name.into(),
            columns: Data::new(Column::new_map(columns_map)),
            column_order: order,
        }
    }

    /// Construct a table with an explicit ordered list of (name, DataType) pairs.
    /// This preserves the ordering exactly as provided.
    pub fn new_with_ordered(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = (impl Into<String>, DataType)>,
    ) -> Self {
        let mut map = HashMap::new();
        let mut order = Vec::new();
        for (n, dt) in columns.into_iter() {
            let name_str = n.into();
            order.push(name_str.clone());
            map.insert(name_str.clone(), dt);
        }
        Self {
            name: name.into(),
            columns: Data::new(Column::new_map(map)),
            column_order: order,
        }
    }

    /// Convenience accessor returning columns in preserved order.
    pub async fn ordered_columns(&self) -> Vec<(String, DataType)> {
        let guard = self.columns.read().await;
        self.column_order
            .iter()
            .filter_map(|n| guard.get(n).map(|c| (n.clone(), c.data_type.clone())))
            .collect()
    }
}
