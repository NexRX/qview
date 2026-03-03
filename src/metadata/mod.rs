crate::reexport!(column);
crate::reexport!(table);
crate::reexport!(schema);
crate::reexport!(database);

use std::{collections::BTreeMap, fmt::Display};

pub type Data<T> = BTreeMap<String, T>;

#[cfg(feature = "backend-impl")]
pub async fn from_postgres_url(url: &str) -> sqlx::Result<Database> {
    use sqlx::{Connection as _, PgConnection};
    tracing::debug!(?url, "Opening connection to database");
    let mut conn = PgConnection::connect(url).await?;
    let database = Database::from_postgres(&mut conn, "postgres").await?;

    Ok(database)
}
