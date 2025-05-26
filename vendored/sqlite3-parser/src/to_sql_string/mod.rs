//! ToSqlString trait definition and implementations

mod stmt;

use crate::ast::TableInternalId;

/// Context to be used in ToSqlString
pub trait ToSqlContext {
    /// Given an id, get the table name
    ///
    /// Currently not considering aliases
    fn get_table_name(&self, id: TableInternalId) -> &str;
}

/// Trait to convert an ast to a string
pub trait ToSqlString {
    /// Convert the given value to String
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String;
}
