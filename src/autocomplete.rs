use crate::*;
use sqlparser::dialect::PostgreSqlDialect;
static POSTGRES: PostgreSqlDialect = PostgreSqlDialect {};
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    start: usize,
    end: Option<usize>,
}

pub fn suggest(sql: &str, cursor: Cursor, metadata: MetaData) -> Result<Vec<String>> {
    let ast = Parser::parse_sql(&POSTGRES, sql)?;

    // Find the statement in the parsed AST that contains the cursor start position.
    let cursor_pos = cursor.start;
    let mut stmt_index: Option<usize> = None;
    let mut stmt_span: Option<(usize, usize)> = None;

    let mut search_chars = 0;
    for (i, stmt) in ast.iter().enumerate() {
        if search_chars >= cursor.start && cursor.end.map(|end| search_chars >= end).unwrap_or(true)
        {
            break; // gone beyond search area
        }
        search_chars += stmt.to_string().len();
        if search_chars > cursor_pos {
            // eventually we will break; if we find what we are looking for
        }
    }

    debug!("Cursor start {cursor_pos} located in statement {stmt_index:?} with span {stmt_span:?}");
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_recommend_for_simple_select() {
        let sql = "SELECT * FROM users";
        let result = suggest(
            sql,
            Cursor {
                start: 8,
                end: None,
            },
        )
        .expect("suggestion shouldnt error");

        assert_eq!(
            result,
            vec!["id".to_string(), "name".to_string(), "password".to_string()]
        );
    }
}
