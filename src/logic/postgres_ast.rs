use derive_more::{Deref, DerefMut};
use sql_parse::*;

#[derive(Debug, Deref, DerefMut)]
pub struct PostgresAST<'a> {
    pub issues: Issues<'a>,
    #[deref]
    #[deref_mut]
    pub statements: Vec<Statement<'a>>,
}

impl<'a> PostgresAST<'a> {
    pub fn parse(sql: &'a str) -> Self {
        let mut issues = Issues::new(sql);
        let options = ParseOptions::new()
            .dialect(SQLDialect::PostgreSQL)
            .arguments(SQLArguments::QuestionMark)
            .warn_unquoted_identifiers(false);
        let statements = parse_statements(sql, &mut issues, &options);
        Self { issues, statements }
    }
}
