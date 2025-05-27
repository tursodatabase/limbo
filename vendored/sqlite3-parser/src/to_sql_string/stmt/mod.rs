use crate::ast;

use super::ToSqlString;

mod select;

impl ToSqlString for ast::Stmt {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::Select(select) => select.to_sql_string(context),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string::ToSqlContext;

    #[macro_export]
    /// Create a test that first parses then input, the converts the parsed ast back to a string and compares with original input
    macro_rules! to_sql_string_test {
        ($test_name:ident, $input:literal) => {
            #[test]
            fn $test_name() {
                let context = crate::to_sql_string::stmt::tests::TestContext;
                let input: &str = $input;
                let mut parser = crate::lexer::sql::Parser::new(input.as_bytes());
                let cmd = fallible_iterator::FallibleIterator::next(&mut parser)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    input,
                    crate::to_sql_string::ToSqlString::to_sql_string(cmd.stmt(), &context)
                );
            }
        };
        ($test_name:ident, $input:literal, $($attribute:meta),*) => {
            #[test]
            $(#[$attribute])*
            fn $test_name() {
                let context = crate::to_sql_string::stmt::tests::TestContext;
                let input: &str = $input;
                let mut parser = crate::lexer::sql::Parser::new(input.as_bytes());
                let cmd = fallible_iterator::FallibleIterator::next(&mut parser)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    input,
                    crate::to_sql_string::ToSqlString::to_sql_string(cmd.stmt(), &context)
                );
            }
        }
    }

    pub(crate) struct TestContext;

    // Placeholders for compilation
    // Context only necessary parsing inside limbo_core or in the simulator
    impl ToSqlContext for TestContext {
        fn get_column_name(&self, _table_id: crate::ast::TableInternalId, _col_idx: usize) -> &str {
            todo!()
        }

        fn get_table_name(&self, _id: crate::ast::TableInternalId) -> &str {
            todo!()
        }
    }
}
