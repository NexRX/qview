crate::reexport!(column);
crate::reexport!(table);
crate::reexport!(schema);
crate::reexport!(database);

use sqlx::{Connection as _, PgConnection};
use std::{collections::BTreeMap, fmt::Display, sync::LazyLock};
use tokio::sync::RwLock;

pub type Data<T> = RwLock<BTreeMap<String, T>>;
pub type DatabaseData = Data<Database>;
pub type MetaData = LazyLock<DatabaseData>;

pub const fn new_metadata() -> MetaData {
    LazyLock::new(|| Data::new(BTreeMap::new()))
}

pub async fn from_postgres_url(url: &str) -> sqlx::Result<MetaData> {
    let mut conn = PgConnection::connect(url).await?;
    let databases: Vec<String> = sqlx::query_scalar!("SELECT datname FROM pg_database")
        .fetch_all(&mut conn)
        .await?;

    let database_metas = new_metadata();
    for database in databases {
        database_metas.blocking_write().insert(
            database.clone(),
            Database::from_postgres(&mut conn, &database).await?,
        );
    }

    Ok(database_metas)
}
