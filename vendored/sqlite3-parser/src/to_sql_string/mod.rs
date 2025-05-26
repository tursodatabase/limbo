//! ToSqlString trait definition and implementations

use crate::ast::TableInternalId;

/// Context to be used in ToSqlString
pub trait ToSqlContext {
    /// Given an id, get the table name
    /// Currently not considering alias
    fn get_table_name(&self, id: TableInternalId) -> &str;
}

/// Trait to convert an ast to a string
pub trait ToSqlString<C: ToSqlContext> {
    /// Convert the given value to String
    fn to_sql_string(&self, context: &C) -> String;
}
