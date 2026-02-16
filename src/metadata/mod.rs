crate::reexport!(column);
crate::reexport!(table);
crate::reexport!(schema);
crate::reexport!(database);

use std::{collections::BTreeMap, fmt::Display, sync::LazyLock};
use tokio::sync::RwLock;

pub type Data<T> = RwLock<BTreeMap<String, T>>;
pub type MetaData = LazyLock<Data<Database>>;
pub static METADATA: MetaData = LazyLock::new(|| Data::new(BTreeMap::new()));

#[cfg(test)]
pub fn new_metadata() -> MetaData {
    LazyLock::new(|| Data::new(BTreeMap::new()))
}
