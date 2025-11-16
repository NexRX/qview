use super::*;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
        }
    }

    pub fn new_map(columns: impl Into<HashMap<String, DataType>>) -> HashMap<String, Self> {
        columns
            .into()
            .into_iter()
            .map(|v| (v.0.clone(), Column::new(v.0, v.1)))
            .collect()
    }
}
