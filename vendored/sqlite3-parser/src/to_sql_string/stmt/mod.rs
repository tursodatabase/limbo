use crate::ast;

use super::ToSqlString;

mod alter_table;
mod create_table;
mod create_trigger;
mod select;

impl ToSqlString for ast::Stmt {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::AlterTable(alter_table) => {
                let (name, body) = alter_table.as_ref();
                format!(
                    "ALTER TABLE {} {};",
                    name.to_sql_string(context),
                    body.to_sql_string(context)
                )
            }
            Self::Analyze(name) => {
                if let Some(name) = name {
                    format!("ANALYZE {};", name.to_sql_string(context))
                } else {
                    format!("ANALYZE;")
                }
            }
            Self::Attach {
                expr,
                db_name,
                key: _,
            } => {
                // TODO: what is `key` in the attach syntax?
                format!(
                    "ATTACH {} AS {};",
                    expr.to_sql_string(context),
                    db_name.to_sql_string(context)
                )
            }
            // TODO: not sure where name is applied here
            // https://www.sqlite.org/lang_transaction.html
            Self::Begin(transaction_type, _name) => {
                let t_type = transaction_type.map_or("", |t_type| match t_type {
                    ast::TransactionType::Deferred => " DEFERRED",
                    ast::TransactionType::Exclusive => " EXCLUSIVE",
                    ast::TransactionType::Immediate => " IMMEDIATE",
                });
                format!("BEGIN{};", t_type)
            }
            // END or COMMIT are equivalent here, so just defaulting to COMMIT
            // TODO: again there are no names in the docs
            Self::Commit(_name) => "COMMIT;".to_string(),
            Self::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                columns,
                where_clause,
            } => {
                format!(
                    "CREATE {}INDEX {}{} ON {} ({}){};",
                    unique.then_some("UNIQUE ").unwrap_or(""),
                    if_not_exists.then_some("IF NOT EXISTS ").unwrap_or(""),
                    idx_name.to_sql_string(context),
                    tbl_name.0,
                    columns
                        .iter()
                        .map(|col| col.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", "),
                    where_clause
                        .as_ref()
                        .map_or("".to_string(), |where_clause| format!(
                            " WHERE {}",
                            where_clause.to_sql_string(context)
                        ))
                )
            }
            Self::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => {
                format!(
                    "CREATE{} TABLE {}{} {};",
                    temporary.then_some(" TEMP").unwrap_or(""),
                    if_not_exists.then_some("IF NOT EXISTS ").unwrap_or(""),
                    tbl_name.to_sql_string(context),
                    body.to_sql_string(context)
                )
            }
            Self::CreateTrigger(trigger) => trigger.to_sql_string(context),
            Self::Select(select) => format!("{};", select.to_sql_string(context)),
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
        ($test_name:ident, $input:expr) => {
            #[test]
            fn $test_name() {
                let context = crate::to_sql_string::stmt::tests::TestContext;
                let input = $input.split_whitespace().collect::<Vec<&str>>().join(" ");
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
        ($test_name:ident, $input:expr, $($attribute:meta),*) => {
            #[test]
            $(#[$attribute])*
            fn $test_name() {
                let context = crate::to_sql_string::stmt::tests::TestContext;
                let input = $input.split_whitespace().collect::<Vec<&str>>().join(" ");
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

    to_sql_string_test!(test_analyze, "ANALYZE;");

    to_sql_string_test!(
        test_analyze_table,
        "ANALYZE table;",
        ignore = "parser can't parse table name"
    );

    to_sql_string_test!(
        test_analyze_schema_table,
        "ANALYZE schema.table;",
        ignore = "parser can't parse schema.table name"
    );

    to_sql_string_test!(test_attach, "ATTACH './test.db' AS test_db;");

    to_sql_string_test!(test_transaction, "BEGIN;");

    to_sql_string_test!(test_transaction_deferred, "BEGIN DEFERRED;");

    to_sql_string_test!(test_transaction_immediate, "BEGIN IMMEDIATE;");

    to_sql_string_test!(test_transaction_exclusive, "BEGIN EXCLUSIVE;");

    to_sql_string_test!(test_commit, "COMMIT;");

    // Test a simple index on a single column
    to_sql_string_test!(
        test_create_index_simple,
        "CREATE INDEX idx_name ON employees (last_name);"
    );

    // Test a unique index to enforce uniqueness on a column
    to_sql_string_test!(
        test_create_unique_index,
        "CREATE UNIQUE INDEX idx_unique_email ON users (email);"
    );

    // Test a multi-column index
    to_sql_string_test!(
        test_create_index_multi_column,
        "CREATE INDEX idx_name_salary ON employees (last_name, salary);"
    );

    // Test a partial index with a WHERE clause
    to_sql_string_test!(
        test_create_partial_index,
        "CREATE INDEX idx_active_users ON users (username) WHERE active = true;"
    );

    // Test an index on an expression
    to_sql_string_test!(
        test_create_index_on_expression,
        "CREATE INDEX idx_upper_name ON employees (UPPER(last_name));"
    );

    // Test an index with descending order
    to_sql_string_test!(
        test_create_index_descending,
        "CREATE INDEX idx_salary_desc ON employees (salary DESC);"
    );

    // Test an index with mixed ascending and descending orders on multiple columns
    to_sql_string_test!(
        test_create_index_mixed_order,
        "CREATE INDEX idx_name_asc_salary_desc ON employees (last_name ASC, salary DESC);"
    );
}
