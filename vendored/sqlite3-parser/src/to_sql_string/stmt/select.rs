use crate::{
    ast,
    to_sql_string::{ToSqlContext, ToSqlString},
};

impl ToSqlString for ast::Select {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        ret
    }
}

impl ToSqlString for ast::SelectBody {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        ret
    }
}

impl ToSqlString for ast::OneSelect {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        match self {
            ast::OneSelect::Select(select) => ret,
            // TODO: come back here when we implement ToSqlString for Expr
            ast::OneSelect::Values(values) => ret,
        }
    }
}

impl ToSqlString for ast::SelectInner {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        ret
    }
}
