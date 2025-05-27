use crate::{ast, to_sql_string::ToSqlString};

impl ToSqlString for ast::AlterTableBody {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::AddColumn(col_def) => format!("ADD COLUMN {}", col_def.to_sql_string(context)),
            Self::DropColumn(name) => format!("DROP COLUMN {}", name.0),
            Self::RenameColumn { old, new } => format!("RENAME COLUMN {} TO {}", old.0, new.0),
            Self::RenameTo(name) => format!("RENAME TO {}", name.0),
        }
    }
}

impl ToSqlString for ast::ColumnDefinition {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "{}{}{}",
            self.col_name.0,
            if let Some(col_type) = &self.col_type {
                format!(" {}", col_type.to_sql_string(context))
            } else {
                "".to_string()
            },
            if !self.constraints.is_empty() {
                self.constraints
                    .iter()
                    .map(|constraint| constraint.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                "".to_string()
            }
        )
    }
}

impl ToSqlString for ast::NamedColumnConstraint {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        let mut ret = Vec::new();
        if let Some(name) = &self.name {
            ret.push(format!("CONSTRAINT {}", name.0));
        }
        ret.push(self.constraint.to_sql_string(context));
        ret.join(" ")
    }
}

impl ToSqlString for ast::ColumnConstraint {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::Check(expr) => format!("CHECK ({})", expr.to_sql_string(context)),
            Self::Collate { collation_name } => format!("COLLATE {}", collation_name.0),
            Self::Default(expr) => {
                if matches!(expr, ast::Expr::Literal(..)) {
                    format!("DEFAULT {}", expr.to_sql_string(context))
                } else {
                    format!("DEFAULT ({})", expr.to_sql_string(context))
                }
            }
            Self::Defer(expr) => expr.to_sql_string(context),
            Self::ForeignKey {
                clause,
                deref_clause,
            } => format!(
                "{}{}",
                clause.to_sql_string(context),
                if let Some(deref) = deref_clause {
                    deref.to_sql_string(context)
                } else {
                    "".to_string()
                }
            ),
            Self::Generated { expr, typ } => {
                format!(
                    "GENERATED ALWAYS AS ({}){}",
                    expr.to_sql_string(context),
                    if let Some(typ) = typ { &typ.0 } else { "" }
                )
            }
            Self::NotNull {
                nullable: _,
                conflict_clause,
            } => {
                // nullable should always be true here
                format!(
                    "NOT NULL{}",
                    conflict_clause.map_or("".to_string(), |conflict| format!(
                        " {}",
                        conflict.to_sql_string(context)
                    ))
                )
            }
            Self::PrimaryKey {
                order,
                conflict_clause,
                auto_increment,
            } => {
                format!(
                    "PRIMARY KEY {}{}{}",
                    order.map_or("".to_string(), |order| format!(
                        " {}",
                        order.to_sql_string(context)
                    )),
                    conflict_clause.map_or("".to_string(), |conflict| format!(
                        " {}",
                        conflict.to_sql_string(context)
                    )),
                    auto_increment.then_some(" AUTOINCREMENT").unwrap_or("")
                )
            }
            Self::Unique(conflict_clause) => {
                format!(
                    "UNIQUE{}",
                    conflict_clause.map_or("".to_string(), |conflict| format!(
                        " {}",
                        conflict.to_sql_string(context)
                    ))
                )
            }
        }
    }
}

impl ToSqlString for ast::ForeignKeyClause {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "REFERENCES {}{}{}",
            self.tbl_name.0,
            if let Some(columns) = &self.columns {
                format!(
                    " ({})",
                    columns
                        .iter()
                        .map(|cols| cols.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                "".to_string()
            },
            if !self.args.is_empty() {
                format!(
                    " {}",
                    self.args
                        .iter()
                        .map(|arg| arg.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            } else {
                "".to_string()
            }
        )
    }
}

impl ToSqlString for ast::RefArg {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::Match(name) => format!("MATCH {}", name.0),
            Self::OnDelete(act) => format!("ON DELETE {}", act.to_sql_string(context)),
            Self::OnUpdate(act) => format!("ON UPDATE {}", act.to_sql_string(context)),
            Self::OnInsert(..) => unimplemented!(
                "On Insert does not exist in SQLite: https://www.sqlite.org/lang_altertable.html"
            ),
        }
    }
}

impl ToSqlString for ast::RefAct {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, _context: &C) -> String {
        match self {
            Self::Cascade => "CASCADE",
            Self::NoAction => "NO ACTION",
            Self::Restrict => "RESTRICT",
            Self::SetDefault => "SET DEFAULT",
            Self::SetNull => "SET NULL",
        }
        .to_string()
    }
}

impl ToSqlString for ast::DeferSubclause {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, _context: &C) -> String {
        format!(
            "{}{}",
            if self.deferrable {
                "NOT DEFERRABLE"
            } else {
                "DEFERRABLE"
            },
            if let Some(init_deffered) = &self.init_deferred {
                match init_deffered {
                    ast::InitDeferredPred::InitiallyDeferred => " INITIALLY DEFERRED",
                    ast::InitDeferredPred::InitiallyImmediate => " INITIALLY IMMEDIATE",
                }
            } else {
                ""
            }
        )
    }
}
