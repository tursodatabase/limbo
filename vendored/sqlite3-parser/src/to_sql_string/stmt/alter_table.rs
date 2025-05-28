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
                format!(
                    " {}",
                    self.constraints
                        .iter()
                        .map(|constraint| constraint.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(" ")
                )
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
            ret.push(format!("CONSTRAINT {} ", name.0));
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
                "FOREIGN KEY {}{}",
                clause.to_sql_string(context),
                if let Some(deref) = deref_clause {
                    deref.to_sql_string(context)
                } else {
                    "".to_string()
                }
            ),
            Self::Generated { expr, typ } => {
                // Don't need to add the generated part
                format!(
                    "AS ({}){}",
                    expr.to_sql_string(context),
                    if let Some(typ) = typ {
                        format!(" {}", &typ.0)
                    } else {
                        "".to_string()
                    }
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
                    "({})",
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
#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(
        test_alter_table_rename,
        "ALTER TABLE t RENAME TO new_table_name"
    );

    to_sql_string_test!(
        test_alter_table_add_column,
        "ALTER TABLE t ADD COLUMN c INTEGER"
    );

    to_sql_string_test!(
        test_alter_table_add_column_with_default,
        "ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'value'"
    );

    to_sql_string_test!(
        test_alter_table_add_column_not_null_default,
        "ALTER TABLE t ADD COLUMN c REAL NOT NULL DEFAULT 0.0"
    );

    to_sql_string_test!(
        test_alter_table_add_column_unique,
        "ALTER TABLE t ADD COLUMN c TEXT UNIQUE",
        ignore = "ParserError = Cannot add a UNIQUE column"
    );

    to_sql_string_test!(
        test_alter_table_rename_column,
        "ALTER TABLE t RENAME COLUMN old_name TO new_name"
    );

    to_sql_string_test!(test_alter_table_drop_column, "ALTER TABLE t DROP COLUMN c");

    to_sql_string_test!(
        test_alter_table_add_column_check,
        "ALTER TABLE t ADD COLUMN c INTEGER CHECK (c > 0)"
    );

    to_sql_string_test!(
        test_alter_table_add_column_foreign_key,
        "ALTER TABLE t ADD COLUMN c INTEGER REFERENCES t2(id) ON DELETE CASCADE"
    );

    to_sql_string_test!(
        test_alter_table_add_column_collate,
        "ALTER TABLE t ADD COLUMN c TEXT COLLATE NOCASE"
    );

    to_sql_string_test!(
        test_alter_table_add_column_primary_key,
        "ALTER TABLE t ADD COLUMN c INTEGER PRIMARY KEY",
        ignore = "ParserError = Cannot add a PRIMARY KEY column"
    );

    to_sql_string_test!(
        test_alter_table_add_column_primary_key_autoincrement,
        "ALTER TABLE t ADD COLUMN c INTEGER PRIMARY KEY AUTOINCREMENT",
        ignore = "ParserError = Cannot add a PRIMARY KEY column"
    );

    to_sql_string_test!(
        test_alter_table_add_generated_column,
        "ALTER TABLE t ADD COLUMN c_generated AS (a + b) STORED"
    );

    to_sql_string_test!(
        test_alter_table_add_column_schema,
        "ALTER TABLE schema_name.t ADD COLUMN c INTEGER"
    );
}
