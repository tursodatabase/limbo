use crate::{ast, to_sql_string::ToSqlString};

impl ToSqlString for ast::CreateVirtualTable {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "CREATE VIRTUAL TABLE {}{} USING {}{};",
            self.if_not_exists.then_some("IF NOT EXISTS ").unwrap_or(""),
            self.tbl_name.to_sql_string(context),
            self.module_name.0,
            self.args
                .as_ref()
                .map_or("".to_string(), |args| format!(" ({})", args.join(", ")))
        )
    }
}
