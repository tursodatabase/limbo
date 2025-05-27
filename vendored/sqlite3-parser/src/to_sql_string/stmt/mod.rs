use crate::ast;

use super::ToSqlString;

mod alter_table;
mod select;

impl ToSqlString for ast::Stmt {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::AlterTable(alter_table) => {
                let (name, body) = alter_table.as_ref();
                format!(
                    "ALTER TABLE {} {}",
                    name.to_sql_string(context),
                    body.to_sql_string(context)
                )
            }
            Self::Analyze(name) => {
                if let Some(name) = name {
                    format!("ANALYZE {}", name.to_sql_string(context))
                } else {
                    format!("ANALYZE")
                }
            }
            Self::Attach {
                expr,
                db_name,
                key: _,
            } => {
                // TODO: what is `key` in the attach syntax?
                format!(
                    "ATTACH {} AS {}",
                    expr.to_sql_string(context),
                    db_name.to_sql_string(context)
                )
            }
            // TODO: not sure where name is applied here
            // https://www.sqlite.org/lang_transaction.html
            Self::Begin(transaction_type, _) => {
                let t_type = transaction_type.map_or("", |t_type| match t_type {
                    ast::TransactionType::Deferred => "DEFERRED ",
                    ast::TransactionType::Exclusive => "EXCLUSIVE ",
                    ast::TransactionType::Immediate => "IMMEDIATE ",
                });
                format!("BEGIN {}TRANSACTION", t_type)
            }
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

    to_sql_string_test!(test_analyze, "ANALYZE");

    to_sql_string_test!(
        test_analyze_table,
        "ANALYZE table",
        ignore = "parser can't parse table name"
    );

    to_sql_string_test!(
        test_analyze_schema_table,
        "ANALYZE schema.table",
        ignore = "parser can't parse schema.table name"
    );

    to_sql_string_test!(test_attach, "ATTACH './test.db' AS test_db");

    to_sql_string_test!(test_transaction, "BEGIN TRANSACTION");

    to_sql_string_test!(test_transaction_deferred, "BEGIN DEFERRED TRANSACTION");

    to_sql_string_test!(test_transaction_immediate, "BEGIN IMMEDIATE TRANSACTION");

    to_sql_string_test!(test_transaction_exclusive, "BEGIN EXCLUSIVE TRANSACTION");
}
