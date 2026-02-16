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
            tables: Data::new(BTreeMap::new()),
        }
    }
}
