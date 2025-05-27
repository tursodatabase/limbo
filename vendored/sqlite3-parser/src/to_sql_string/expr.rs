use crate::ast::{self, Expr};

use super::ToSqlString;

impl ToSqlString for Expr {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        match self {
            Expr::Between {
                lhs,
                not,
                start,
                end,
            } => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push(' ');

                if *not {
                    ret.push_str("NOT ");
                }

                ret.push_str("BETWEEN ");

                ret.push_str(&start.to_sql_string(context));

                ret.push_str(" AND ");

                ret.push_str(&end.to_sql_string(context));
            }
            Expr::Binary(lhs, op, rhs) => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push(' ');
                ret.push_str(&op.to_sql_string(context));
                ret.push(' ');
                ret.push_str(&rhs.to_sql_string(context));
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                ret.push_str("CASE ");
                if let Some(base) = base {
                    ret.push_str(&base.to_sql_string(context));
                }
                for (when, then) in when_then_pairs {
                    ret.push_str(" WHEN ");
                    ret.push_str(&when.to_sql_string(context));
                    ret.push_str(" THEN ");
                    ret.push_str(&then.to_sql_string(context));
                }
                if let Some(else_expr) = else_expr {
                    ret.push_str(" ELSE ");
                    ret.push_str(&else_expr.to_sql_string(context));
                }
                ret.push_str(" END");
            }
            Expr::Cast { expr, type_name } => {
                ret.push_str("CAST");
                ret.push('(');
                ret.push_str(&expr.to_sql_string(context));
                if let Some(type_name) = type_name {
                    ret.push_str(" AS ");
                    ret.push_str(&type_name.to_sql_string(context));
                }
                ret.push(')');
            }
            Expr::Collate(expr, name) => {
                ret.push_str(&expr.to_sql_string(context));
                ret.push_str(" COLLATE ");
                ret.push_str(&name);
            }
            Expr::DoublyQualified(name, name1, name2) => {
                ret.push_str(&name.0);
                ret.push('.');
                ret.push_str(&name1.0);
                ret.push('.');
                ret.push_str(&name2.0);
            }
            Expr::Exists(select) => {
                ret.push_str("EXISTS (");
                ret.push_str(&select.to_sql_string(context));
                ret.push(')');
            }
            Expr::FunctionCall {
                name,
                distinctness: _,
                args,
                order_by: _,
                filter_over,
            } => {
                ret.push_str(&name.0);
                // TODO: pretty sure there should be no ORDER_BY nor DISTINCT
                ret.push('(');
                if let Some(args) = args {
                    let joined_args = args
                        .iter()
                        .map(|arg| arg.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ");
                    ret.push_str(&joined_args);
                }
                ret.push(')');
                if let Some(filter_over) = filter_over {
                    if let Some(filter) = &filter_over.filter_clause {
                        ret.push_str(" FILTER (");
                        ret.push_str("WHERE ");
                        ret.push_str(&filter.to_sql_string(context));
                        ret.push(')');
                    }
                    if let Some(_over) = &filter_over.over_clause {
                        todo!()
                    }
                }
            }
            Expr::FunctionCallStar { name, filter_over } => {
                ret.push_str(&name.0);
                ret.push_str("(*)");
                if let Some(filter_over) = filter_over {
                    if let Some(filter) = &filter_over.filter_clause {
                        ret.push_str(" FILTER (");
                        ret.push_str("WHERE ");
                        ret.push_str(&filter.to_sql_string(context));
                        ret.push(')');
                    }
                    if let Some(_over) = &filter_over.over_clause {
                        todo!()
                    }
                }
            }
            Expr::Id(id) => {
                ret.push_str(&id.0);
            }
            Expr::Column {
                database: _, // TODO: Ignore database for now
                table,
                column,
                is_rowid_alias: _,
            } => {
                ret.push_str(context.get_table_name(*table));
                ret.push('.');
                ret.push_str(context.get_column_name(*table, *column));
            }
            // TODO: not sure how to rewrite this
            Expr::RowId {
                database: _,
                table: _,
            } => todo!(),
            Expr::InList { lhs, not, rhs } => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push(' ');
                if *not {
                    ret.push_str("NOT ");
                }
                ret.push('(');
                if let Some(rhs) = rhs {
                    let joined_args = rhs
                        .iter()
                        .map(|expr| expr.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ");
                    ret.push_str(&joined_args);
                }
                ret.push(')');
            }
            Expr::InSelect { lhs, not, rhs } => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push(' ');
                if *not {
                    ret.push_str("NOT ");
                }
                ret.push('(');
                ret.push_str(&rhs.to_sql_string(context));
                ret.push(')');
            }
            Expr::InTable {
                lhs,
                not,
                rhs,
                args,
            } => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push(' ');
                if *not {
                    ret.push_str("NOT ");
                }
                ret.push_str(&rhs.to_sql_string(context));

                if let Some(args) = args {
                    ret.push('(');
                    let joined_args = args
                        .iter()
                        .map(|expr| expr.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ");
                    ret.push_str(&joined_args);
                    ret.push(')');
                }
            }
            Expr::IsNull(expr) => {
                ret.push_str(&expr.to_sql_string(context));
                ret.push_str(" ISNULL");
            }
            Expr::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push(' ');
                if *not {
                    ret.push_str("NOT ");
                }
                ret.push_str(&op.to_sql_string(context));
                ret.push(' ');
                ret.push_str(&rhs.to_sql_string(context));
                if let Some(escape) = escape {
                    ret.push_str(" ESCAPE ");
                    ret.push_str(&escape.to_sql_string(context));
                }
            }
            Expr::Literal(literal) => {
                ret.push_str(&literal.to_sql_string(context));
            }
            Expr::Name(name) => {
                ret.push_str(&name.0);
            }
            Expr::NotNull(expr) => {
                ret.push_str(&expr.to_sql_string(context));
                ret.push_str(" NOT NULL");
            }
            Expr::Parenthesized(exprs) => {
                ret.push('(');
                let joined_args = exprs
                    .iter()
                    .map(|expr| expr.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(", ");
                ret.push_str(&joined_args);
                ret.push(')');
            }
            Expr::Qualified(name, name1) => {
                ret.push_str(&name.0);
                ret.push('.');
                ret.push_str(&name1.0);
            }
            Expr::Raise(resolve_type, expr) => {
                ret.push_str("RAISE (");
                ret.push_str(&resolve_type.to_sql_string(context));
                if let Some(expr) = expr {
                    ret.push_str(", ");
                    ret.push_str(&expr.to_sql_string(context));
                }
                ret.push(')');
            }
            Expr::Subquery(select) => {
                ret.push('(');
                ret.push_str(&select.to_sql_string(context));
                ret.push(')');
            }
            Expr::Unary(unary_operator, expr) => {
                ret.push_str(&unary_operator.to_sql_string(context));
                ret.push(' ');
                ret.push_str(&expr.to_sql_string(context));
            }
            Expr::Variable(variable) => {
                ret.push_str(variable);
            }
        };
        ret
    }
}

impl ToSqlString for ast::Operator {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::Add => "+",
            Self::And => "AND",
            Self::ArrowRight => "->",
            Self::ArrowRightShift => "->>",
            Self::BitwiseAnd => "&",
            Self::BitwiseNot => "~",
            Self::BitwiseOr => "|",
            Self::Concat => "||",
            Self::Divide => "/",
            Self::Equals => "==",
            Self::Greater => ">",
            Self::GreaterEquals => ">=",
            Self::Is => "IS",
            Self::IsNot => "IS NOT",
            Self::LeftShift => "<<",
            Self::Less => "<",
            Self::LessEquals => "<=",
            Self::Modulus => "%",
            Self::Multiply => "*",
            Self::NotEquals => "!=",
            Self::Or => "OR",
            Self::RightShift => ">>",
            Self::Subtract => "-",
        }
        .to_string()
    }
}

impl ToSqlString for ast::Type {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        let mut ret = self.name.clone();
        ret.push(' ');
        if let Some(size) = &self.size {
            ret.push('(');
            ret.push_str(&size.to_sql_string(context));
            ret.push(')');
        }
        ret
    }
}

impl ToSqlString for ast::TypeSize {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        let mut ret = String::new();
        match self {
            Self::MaxSize(e) => {
                ret.push_str(&e.to_sql_string(context));
            }
            Self::TypeSize(lhs, rhs) => {
                ret.push_str(&lhs.to_sql_string(context));
                ret.push_str(", ");
                ret.push_str(&rhs.to_sql_string(context));
            }
        };
        ret
    }
}

impl ToSqlString for ast::Distinctness {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::All => "ALL",
            Self::Distinct => "DISTINCT",
        }
        .to_string()
    }
}

impl ToSqlString for ast::QualifiedName {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        let mut ret = String::new();
        // TODO: skip DB Name
        if let Some(alias) = &self.alias {
            ret.push_str(&alias.0);
            ret.push('.');
        }
        ret.push_str(&self.name.0);
        ret
    }
}

impl ToSqlString for ast::LikeOperator {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::Glob => "GLOB",
            Self::Like => "LIKE",
            Self::Match => "MATCH",
            Self::Regexp => "REGEXP",
        }
        .to_string()
    }
}

impl ToSqlString for ast::Literal {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::Blob(b) => format!("Ox{b}"),
            Self::CurrentDate => "CURRENT_DATE".to_string(),
            Self::CurrentTime => "CURRENT_TIME".to_string(),
            Self::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            Self::Keyword(keyword) => keyword.clone(),
            Self::Null => "NULL".to_string(),
            Self::Numeric(num) => num.clone(),
            Self::String(s) => s.clone(),
        }
    }
}

impl ToSqlString for ast::ResolveType {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::Abort => "ABORT",
            Self::Fail => "FAIL",
            Self::Ignore => "IGNORE",
            Self::Replace => "REPLACE",
            Self::Rollback => "ROLLBACK",
        }
        .to_string()
    }
}

impl ToSqlString for ast::UnaryOperator {
    fn to_sql_string<C: super::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::BitwiseNot => "~",
            Self::Negative => "-",
            Self::Not => "NOT",
            Self::Positive => "+",
        }
        .to_string()
    }
}
