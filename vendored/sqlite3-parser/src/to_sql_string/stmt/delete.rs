use crate::{ast, to_sql_string::ToSqlString};

impl ToSqlString for ast::Delete {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "{}DELETE FROM {}{}{}{}{}{};",
            self.with.as_ref().map_or("".to_string(), |with| format!(
                "{} ",
                with.to_sql_string(context)
            )),
            self.tbl_name.to_sql_string(context),
            self.indexed
                .as_ref()
                .map_or("".to_string(), |indexed| format!(
                    " {}",
                    indexed.to_sql_string(context)
                )),
            self.where_clause
                .as_ref()
                .map_or("".to_string(), |expr| format!(
                    " {}",
                    expr.to_sql_string(context)
                )),
            self.returning
                .as_ref()
                .map_or("".to_string(), |returning| format!(
                    " {}",
                    returning
                        .iter()
                        .map(|col| col.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            self.order_by
                .as_ref()
                .map_or("".to_string(), |order_by| format!(
                    " ORDER BY {}",
                    order_by
                        .iter()
                        .map(|col| col.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            self.limit.as_ref().map_or("".to_string(), |limit| format!(
                " {}",
                limit.to_sql_string(context)
            ))
        )
    }
}
