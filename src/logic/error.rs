use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database error [{code}]: {kind}({message})")]
    Database {
        kind: sqlx_cut::ErrorKind,
        message: String,
        code: String,
        position: Option<sqlx_cut::PgErrorPosition>,
    },

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub mod sqlx_cut {
    use derive_more::Display;

    use super::*;
    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum PgErrorPosition {
        /// A position (in characters) into the original query.
        Original(usize),

        /// A position into the internally-generated query.
        Internal {
            /// The position in characters.
            position: usize,
            /// The text of a failed internally-generated command. This could be, for example,
            /// the SQL query issued by a PL/pgSQL function.
            query: String,
        },
    }

    #[cfg(any(feature = "backend-impl", test))]
    impl From<sqlx::postgres::PgErrorPosition<'_>> for PgErrorPosition {
        fn from(value: sqlx::postgres::PgErrorPosition<'_>) -> Self {
            match value {
                sqlx::postgres::PgErrorPosition::Original(position) => {
                    PgErrorPosition::Original(position)
                }
                sqlx::postgres::PgErrorPosition::Internal { position, query } => {
                    PgErrorPosition::Internal {
                        position,
                        query: query.to_string(),
                    }
                }
            }
        }
    }

    /// The error kind.
    ///
    /// This enum is to be used to identify frequent errors that can be handled by the program.
    /// Although it currently only supports constraint violations, the type may grow in the future.
    #[derive(Debug, PartialEq, Eq, Display)]
    #[non_exhaustive]
    pub enum ErrorKind {
        /// Unique/primary key constraint violation.
        UniqueViolation,
        /// Foreign key constraint violation.
        ForeignKeyViolation,
        /// Not-null constraint violation.
        NotNullViolation,
        /// Check constraint violation.
        CheckViolation,
        /// Exclusion constraint violation.
        ExclusionViolation,
        /// An unmapped error.
        Other,
    }

    #[cfg(any(feature = "backend-impl", test))]
    impl From<sqlx::error::ErrorKind> for ErrorKind {
        fn from(value: sqlx::error::ErrorKind) -> Self {
            match value {
                sqlx::error::ErrorKind::UniqueViolation => ErrorKind::UniqueViolation,
                sqlx::error::ErrorKind::ForeignKeyViolation => ErrorKind::ForeignKeyViolation,
                sqlx::error::ErrorKind::NotNullViolation => ErrorKind::NotNullViolation,
                sqlx::error::ErrorKind::CheckViolation => ErrorKind::CheckViolation,
                sqlx::error::ErrorKind::ExclusionViolation => ErrorKind::ExclusionViolation,
                sqlx::error::ErrorKind::Other => ErrorKind::Other,
                _ => ErrorKind::Other,
            }
        }
    }
}

pub type Result<T = ()> = std::result::Result<T, Error>;

#[cfg(any(feature = "backend-impl", test))]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::Database(db_err) => Error::Database {
                kind: db_err.kind().into(),
                message: db_err.message().to_string(),
                code: db_err
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "Unknown".to_string()),
                position: db_err
                    .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                    .and_then(|v| v.position())
                    .map(sqlx_cut::PgErrorPosition::from),
            },
            sqlx::Error::Io(io_err) => Error::Io(io_err),
            sqlx::Error::PoolTimedOut => Error::Connection("Pool timed out".to_string()),
            sqlx::Error::PoolClosed => Error::Connection("Pool closed".to_string()),
            sqlx::Error::Configuration(msg) => Error::Config(msg.to_string()),
            _ => Error::Internal(err.to_string()),
        }
    }
}
