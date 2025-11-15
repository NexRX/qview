use sqlparser::parser::ParserError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid query: {0}")]
    InvalidQuery(ParserError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T = ()> = std::result::Result<T, Error>;

impl From<ParserError> for Error {
    fn from(value: ParserError) -> Self {
        Error::InvalidQuery(value)
    }
}
