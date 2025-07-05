use core::fmt;
use std::fmt::{Display, Formatter};

use turso_sqlite3_parser::{
    ast::{
        self,
        fmt::{ToTokens, TokenStream},
        SortOrder, TableInternalId,
    },
    dialect::TokenType,
    to_sql_string::{ToSqlContext, ToSqlString},
};

use crate::{schema::Table, translate::plan::TableReferences};

use super::plan::{
    Aggregate, DeletePlan, JoinedTable, Operation, Plan, ResultSetColumn, Search, SelectPlan,
    UpdatePlan,
};

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{:?}({})", self.func, args_str)
    }
}

/// For EXPLAIN QUERY PLAN
impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(select_plan) => select_plan.fmt(f),
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                for (plan, operator) in left {
                    plan.fmt(f)?;
                    writeln!(f, "{}", operator)?;
                }
                right_most.fmt(f)?;
                if let Some(limit) = limit {
                    writeln!(f, "LIMIT: {}", limit)?;
                }
                if let Some(offset) = offset {
                    writeln!(f, "OFFSET: {}", offset)?;
                }
                if let Some(order_by) = order_by {
                    writeln!(f, "ORDER BY:")?;
                    for (expr, dir) in order_by {
                        writeln!(
                            f,
                            "  - {} {}",
                            expr,
                            if *dir == SortOrder::Asc {
                                "ASC"
                            } else {
                                "DESC"
                            }
                        )?;
                    }
                }
                Ok(())
            }
            Self::Delete(delete_plan) => delete_plan.fmt(f),
            Self::Update(update_plan) => update_plan.fmt(f),
        }
    }
}

impl Display for SelectPlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Print each table reference with appropriate indentation based on join depth
        for (i, reference) in self.table_references.joined_tables().iter().enumerate() {
            let is_last = i == self.table_references.joined_tables().len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan { .. } => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    writeln!(f, "{}SCAN {}", indent, table_name)?;
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
            }
        }
        Ok(())
    }
}

impl Display for DeletePlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Delete plan should only have one table reference
        if let Some(reference) = self.table_references.joined_tables().first() {
            let indent = "`--";

            match &reference.op {
                Operation::Scan { .. } => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    writeln!(f, "{}DELETE FROM {}", indent, table_name)?;
                }
                Operation::Search { .. } => {
                    panic!("DELETE plans should not contain search operations");
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for UpdatePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        for (i, reference) in self.table_references.joined_tables().iter().enumerate() {
            let is_last = i == self.table_references.joined_tables().len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan { .. } => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    if i == 0 {
                        writeln!(f, "{}UPDATE {}", indent, table_name)?;
                    } else {
                        writeln!(f, "{}SCAN {}", indent, table_name)?;
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
            }
        }
        if let Some(order_by) = &self.order_by {
            writeln!(f, "ORDER BY:")?;
            for (expr, dir) in order_by {
                writeln!(
                    f,
                    "  - {} {}",
                    expr,
                    if *dir == SortOrder::Asc {
                        "ASC"
                    } else {
                        "DESC"
                    }
                )?;
            }
        }
        if let Some(limit) = self.limit {
            writeln!(f, "LIMIT: {}", limit)?;
        }
        if let Some(ret) = &self.returning {
            writeln!(f, "RETURNING:")?;
            for col in ret {
                writeln!(f, "  - {}", col.expr)?;
            }
        }

        Ok(())
    }
}

pub struct PlanContext<'a>(pub &'a [&'a TableReferences]);

// Definitely not perfect yet
impl ToSqlContext for PlanContext<'_> {
    fn get_column_name(&self, table_id: TableInternalId, col_idx: usize) -> &str {
        let table = self
            .0
            .iter()
            .map(|table_ref| table_ref.find_table_by_internal_id(table_id))
            .reduce(|accum, curr| match (accum, curr) {
                (Some(table), _) | (_, Some(table)) => Some(table),
                _ => None,
            })
            .unwrap()
            .unwrap();
        let cols = table.columns();
        cols.get(col_idx).unwrap().name.as_ref().unwrap()
    }

    fn get_table_name(&self, id: TableInternalId) -> &str {
        let table_ref = self
            .0
            .iter()
            .find(|table_ref| table_ref.find_table_by_internal_id(id).is_some())
            .unwrap();
        let joined_table = table_ref.find_joined_table_by_internal_id(id);
        let outer_query = table_ref.find_outer_query_ref_by_internal_id(id);
        match (joined_table, outer_query) {
            (Some(table), None) => &table.identifier,
            (None, Some(table)) => &table.identifier,
            _ => unreachable!(),
        }
    }
}

impl ToTokens for Plan {
    fn to_tokens_with_context<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        match self {
            Self::Select(select) => {
                select.to_tokens_with_context(s, &PlanContext(&[&select.table_references]))?;
            }
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                let all_refs = left
                    .iter()
                    .flat_map(|(plan, _)| std::iter::once(&plan.table_references))
                    .chain(std::iter::once(&right_most.table_references))
                    .collect::<Vec<_>>();
                let context = &PlanContext(all_refs.as_slice());

                for (plan, operator) in left {
                    plan.to_tokens_with_context(s, context)?;
                    operator.to_tokens_with_context(s, context)?;
                }

                right_most.to_tokens_with_context(s, context)?;

                if let Some(order_by) = order_by {
                    s.append(TokenType::TK_ORDER, None)?;
                    s.append(TokenType::TK_BY, None)?;

                    s.comma(
                        order_by.iter().map(|(expr, order)| ast::SortedColumn {
                            expr: expr.clone(),
                            order: Some(*order),
                            nulls: None,
                        }),
                        context,
                    )?;
                }

                if let Some(limit) = &limit {
                    s.append(TokenType::TK_LIMIT, None)?;
                    s.append(TokenType::TK_FLOAT, Some(&limit.to_string()))?;
                }

                if let Some(offset) = &offset {
                    s.append(TokenType::TK_OFFSET, None)?;
                    s.append(TokenType::TK_FLOAT, Some(&offset.to_string()))?;
                }
            }
            Self::Delete(delete) => delete.to_tokens_with_context(s, context)?,
            Self::Update(update) => update.to_tokens_with_context(s, context)?,
        }

        Ok(())
    }
}

impl ToSqlString for Plan {
    fn to_sql_string<C: ToSqlContext>(&self, context: &C) -> String {
        // Make the Plans pass their own context
        match self {
            Self::Select(select) => select.to_sql_string(&PlanContext(&[&select.table_references])),
            Self::CompoundSelect {
                left,
                right_most,
                limit,
                offset,
                order_by,
            } => {
                let all_refs = left
                    .iter()
                    .flat_map(|(plan, _)| std::iter::once(&plan.table_references))
                    .chain(std::iter::once(&right_most.table_references))
                    .collect::<Vec<_>>();
                let context = &PlanContext(all_refs.as_slice());

                let mut ret = Vec::new();
                for (plan, operator) in left {
                    ret.push(format!("{} {}", plan.to_sql_string(context), operator));
                }
                ret.push(right_most.to_sql_string(context));
                if let Some(order_by) = &order_by {
                    ret.push(format!(
                        "ORDER BY {}",
                        order_by
                            .iter()
                            .map(|(expr, order)| format!(
                                "{} {}",
                                expr.to_sql_string(context),
                                order
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if let Some(limit) = &limit {
                    ret.push(format!("LIMIT {}", limit));
                }
                if let Some(offset) = &offset {
                    ret.push(format!("OFFSET {}", offset));
                }
                ret.join(" ")
            }
            Self::Delete(delete) => delete.to_sql_string(context),
            Self::Update(update) => update.to_sql_string(context),
        }
    }
}

impl ToTokens for JoinedTable {
    fn to_tokens_with_context<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _context: &C,
    ) -> Result<(), S::Error> {
        match &self.table {
            Table::BTree(..) | Table::Virtual(..) => {
                let name = self.table.get_name();
                s.append(TokenType::TK_ID, Some(name))?;
                if self.identifier != name {
                    s.append(TokenType::TK_AS, None)?;
                    s.append(TokenType::TK_ID, Some(&self.identifier))?;
                }
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                s.append(TokenType::TK_LP, None)?;
                // Could possibly merge the contexts together here
                from_clause_subquery.plan.to_tokens_with_context(
                    s,
                    &PlanContext(&[&from_clause_subquery.plan.table_references]),
                )?;
                s.append(TokenType::TK_RP, None)?;

                s.append(TokenType::TK_AS, None)?;
                s.append(TokenType::TK_ID, Some(&self.identifier))?;
            }
        };

        Ok(())
    }
}

impl ToSqlString for JoinedTable {
    fn to_sql_string<C: turso_sqlite3_parser::to_sql_string::ToSqlContext>(
        &self,
        _context: &C,
    ) -> String {
        let table_or_subquery =
            match &self.table {
                Table::BTree(..) | Table::Virtual(..) => self.table.get_name().to_string(),
                Table::FromClauseSubquery(from_clause_subquery) => {
                    // Could possibly merge the contexts together here
                    format!(
                        "({})",
                        from_clause_subquery.plan.to_sql_string(&PlanContext(&[
                            &from_clause_subquery.plan.table_references
                        ]))
                    )
                }
            };
        // JOIN is done at a higher level
        format!(
            "{}{}",
            table_or_subquery,
            if self.identifier != table_or_subquery {
                format!(" AS {}", self.identifier)
            } else {
                "".to_string()
            }
        )
    }
}

impl ToTokens for SelectPlan {
    fn to_tokens_with_context<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        context: &C,
    ) -> Result<(), S::Error> {
        if !self.values.is_empty() {
            ast::OneSelect::Values(self.values.clone()).to_tokens_with_context(s, context)?;
        } else {
            s.append(TokenType::TK_SELECT, None)?;
            if self.distinctness.is_distinct() {
                s.append(TokenType::TK_DISTINCT, None)?;
            }

            for (i, ResultSetColumn { expr, alias, .. }) in self.result_columns.iter().enumerate() {
                if i != 0 {
                    s.append(TokenType::TK_COMMA, None)?;
                }

                expr.to_tokens_with_context(s, context)?;
                if let Some(alias) = alias {
                    s.append(TokenType::TK_AS, None)?;
                    s.append(TokenType::TK_ID, Some(&alias))?;
                }
            }
            s.append(TokenType::TK_FROM, None)?;

            for (i, order) in self.join_order.iter().enumerate() {
                if i != 0 {
                    if order.is_outer {
                        s.append(TokenType::TK_ORDER, None)?;
                    }
                    s.append(TokenType::TK_JOIN, None)?;
                }

                let table_ref = self.joined_tables().get(order.original_idx).unwrap();
                table_ref.to_tokens_with_context(s, context)?;
            }

            if !self.where_clause.is_empty() {
                s.append(TokenType::TK_WHERE, None)?;

                for (i, expr) in self
                    .where_clause
                    .iter()
                    .map(|where_clause| where_clause.expr.clone())
                    .enumerate()
                {
                    if i != 0 {
                        s.append(TokenType::TK_AND, None)?;
                    }
                    expr.to_tokens_with_context(s, context)?;
                }
            }

            if let Some(group_by) = &self.group_by {
                s.append(TokenType::TK_GROUP, None)?;
                s.append(TokenType::TK_BY, None)?;

                s.comma(group_by.exprs.iter(), context)?;

                // TODO: not sure where I need to place the group_by.sort_order
                if let Some(having) = &group_by.having {
                    s.append(TokenType::TK_HAVING, None)?;

                    for (i, expr) in having.iter().enumerate() {
                        if i != 0 {
                            s.append(TokenType::TK_AND, None)?;
                        }
                        expr.to_tokens_with_context(s, context)?;
                    }
                }
            }
        }

        if let Some(order_by) = &self.order_by {
            s.append(TokenType::TK_ORDER, None)?;
            s.append(TokenType::TK_BY, None)?;

            s.comma(
                order_by.iter().map(|(expr, order)| ast::SortedColumn {
                    expr: expr.clone(),
                    order: Some(*order),
                    nulls: None,
                }),
                context,
            )?;
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            s.append(TokenType::TK_FLOAT, Some(&limit.to_string()))?;
        }

        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            s.append(TokenType::TK_FLOAT, Some(&offset.to_string()))?;
        }

        Ok(())
    }
}

// TODO: currently cannot print the original CTE as it is optimized into a subquery
impl ToSqlString for SelectPlan {
    fn to_sql_string<C: turso_sqlite3_parser::to_sql_string::ToSqlContext>(
        &self,
        context: &C,
    ) -> String {
        let mut ret = Vec::new();
        // VALUES SELECT statement
        if !self.values.is_empty() {
            ret.push(format!(
                "VALUES {}",
                self.values
                    .iter()
                    .map(|value| {
                        let joined_value = value
                            .iter()
                            .map(|e| e.to_sql_string(context))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", joined_value)
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        } else {
            // standard SELECT statement
            ret.push("SELECT".to_string());
            if self.distinctness.is_distinct() {
                ret.push("DISTINCT".to_string());
            }
            ret.push(
                self.result_columns
                    .iter()
                    .map(|cols| {
                        format!(
                            "{}{}",
                            cols.expr.to_sql_string(context),
                            cols.alias
                                .as_ref()
                                .map_or("".to_string(), |alias| format!(" AS {}", alias))
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            ret.push("FROM".to_string());

            ret.extend(self.join_order.iter().enumerate().map(|(idx, order)| {
                let table_ref = self.joined_tables().get(order.original_idx).unwrap();
                if idx == 0 {
                    table_ref.to_sql_string(context)
                } else {
                    format!(
                        "{}JOIN {}",
                        if order.is_outer { "OUTER " } else { "" },
                        table_ref.to_sql_string(context)
                    )
                }
            }));
            if !self.where_clause.is_empty() {
                ret.push("WHERE".to_string());
                ret.push(
                    self.where_clause
                        .iter()
                        .map(|where_clause| where_clause.expr.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            if let Some(group_by) = &self.group_by {
                // TODO: see later if group_by needs more context to parse the expressions
                // We will see this when this panics
                ret.push("GROUP BY".to_string());
                ret.push(
                    group_by
                        .exprs
                        .iter()
                        .map(|expr| expr.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                // TODO: not sure where I need to place the group_by.sort_order
                if let Some(having) = &group_by.having {
                    ret.push("HAVING".to_string());
                    ret.push(
                        having
                            .iter()
                            .map(|expr| expr.to_sql_string(context))
                            .collect::<Vec<_>>()
                            .join(" AND "),
                    );
                }
            }
        }
        if let Some(order_by) = &self.order_by {
            ret.push(format!(
                "ORDER BY {}",
                order_by
                    .iter()
                    .map(|(expr, order)| format!("{} {}", expr.to_sql_string(context), order))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = &self.limit {
            ret.push(format!("LIMIT {}", limit));
        }
        if let Some(offset) = &self.offset {
            ret.push(format!("OFFSET {}", offset));
        }
        ret.join(" ")
    }
}

impl ToTokens for DeletePlan {
    fn to_tokens_with_context<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        let table = self
            .table_references
            .joined_tables()
            .first()
            .expect("Delete Plan should have only one table reference");
        let context = &[&self.table_references];
        let context = &PlanContext(context);

        s.append(TokenType::TK_DELETE, None)?;
        s.append(TokenType::TK_FROM, None)?;
        s.append(TokenType::TK_ID, Some(table.table.get_name()))?;

        if !self.where_clause.is_empty() {
            s.append(TokenType::TK_WHERE, None)?;

            for (i, expr) in self
                .where_clause
                .iter()
                .map(|where_clause| where_clause.expr.clone())
                .enumerate()
            {
                if i != 0 {
                    s.append(TokenType::TK_AND, None)?;
                }
                expr.to_tokens_with_context(s, context)?;
            }
        }

        if let Some(order_by) = &self.order_by {
            s.append(TokenType::TK_ORDER, None)?;
            s.append(TokenType::TK_BY, None)?;

            s.comma(
                order_by.iter().map(|(expr, order)| ast::SortedColumn {
                    expr: expr.clone(),
                    order: Some(*order),
                    nulls: None,
                }),
                context,
            )?;
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            s.append(TokenType::TK_FLOAT, Some(&limit.to_string()))?;
        }

        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            s.append(TokenType::TK_FLOAT, Some(&offset.to_string()))?;
        }

        Ok(())
    }
}

impl ToSqlString for DeletePlan {
    fn to_sql_string<C: ToSqlContext>(&self, _context: &C) -> String {
        let table = self
            .table_references
            .joined_tables()
            .first()
            .expect("Delete Plan should have only one table reference");
        let context = &[&self.table_references];
        let context = &PlanContext(context);
        let mut ret = Vec::new();

        ret.push(format!("DELETE FROM {}", table.table.get_name()));

        if !self.where_clause.is_empty() {
            ret.push("WHERE".to_string());
            ret.push(
                self.where_clause
                    .iter()
                    .map(|where_clause| where_clause.expr.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(" AND "),
            );
        }
        if let Some(order_by) = &self.order_by {
            ret.push(format!(
                "ORDER BY {}",
                order_by
                    .iter()
                    .map(|(expr, order)| format!("{} {}", expr.to_sql_string(context), order))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = &self.limit {
            ret.push(format!("LIMIT {}", limit));
        }
        if let Some(offset) = &self.offset {
            ret.push(format!("OFFSET {}", offset));
        }
        ret.join(" ")
    }
}

impl ToTokens for UpdatePlan {
    fn to_tokens_with_context<S: TokenStream + ?Sized, C: ToSqlContext>(
        &self,
        s: &mut S,
        _: &C,
    ) -> Result<(), S::Error> {
        let table = self
            .table_references
            .joined_tables()
            .first()
            .expect("UPDATE Plan should have only one table reference");
        let context = [&self.table_references];
        let context = &PlanContext(&context);

        s.append(TokenType::TK_UPDATE, None)?;
        s.append(TokenType::TK_ID, Some(table.table.get_name()))?;
        s.append(TokenType::TK_SET, None)?;

        s.comma(
            self.set_clauses.iter().map(|(col_idx, set_expr)| {
                let col_name = table
                    .table
                    .get_column_at(*col_idx)
                    .as_ref()
                    .unwrap()
                    .name
                    .as_ref()
                    .unwrap();

                ast::Set {
                    col_names: ast::DistinctNames::single(ast::Name(col_name.clone())),
                    expr: set_expr.clone(),
                }
            }),
            context,
        )?;

        if !self.where_clause.is_empty() {
            s.append(TokenType::TK_WHERE, None)?;

            let mut iter = self
                .where_clause
                .iter()
                .map(|where_clause| where_clause.expr.clone());
            iter.next()
                .expect("should not be empty")
                .to_tokens_with_context(s, context)?;
            for expr in iter {
                s.append(TokenType::TK_AND, None)?;
                expr.to_tokens_with_context(s, context)?;
            }
        }

        if let Some(order_by) = &self.order_by {
            s.append(TokenType::TK_ORDER, None)?;
            s.append(TokenType::TK_BY, None)?;

            s.comma(
                order_by.iter().map(|(expr, order)| ast::SortedColumn {
                    expr: expr.clone(),
                    order: Some(*order),
                    nulls: None,
                }),
                context,
            )?;
        }

        if let Some(limit) = &self.limit {
            s.append(TokenType::TK_LIMIT, None)?;
            s.append(TokenType::TK_FLOAT, Some(&limit.to_string()))?;
        }
        if let Some(offset) = &self.offset {
            s.append(TokenType::TK_OFFSET, None)?;
            s.append(TokenType::TK_FLOAT, Some(&offset.to_string()))?;
        }

        Ok(())
    }
}

impl ToSqlString for UpdatePlan {
    fn to_sql_string<C: ToSqlContext>(&self, _context: &C) -> String {
        let table = self
            .table_references
            .joined_tables()
            .first()
            .expect("UPDATE Plan should have only one table reference");
        let context = [&self.table_references];
        let context = &PlanContext(&context);
        let mut ret = Vec::new();

        // TODO: we don't work with conflict clauses yet

        ret.push(format!("UPDATE {} SET", table.table.get_name()));

        // TODO: does not support column_name_list yet
        ret.push(
            self.set_clauses
                .iter()
                .map(|(col_idx, set_expr)| {
                    format!(
                        "{} = {}",
                        table
                            .table
                            .get_column_at(*col_idx)
                            .as_ref()
                            .unwrap()
                            .name
                            .as_ref()
                            .unwrap(),
                        set_expr.to_sql_string(context)
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );

        if !self.where_clause.is_empty() {
            ret.push("WHERE".to_string());
            ret.push(
                self.where_clause
                    .iter()
                    .map(|where_clause| where_clause.expr.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(" AND "),
            );
        }
        if let Some(order_by) = &self.order_by {
            ret.push(format!(
                "ORDER BY {}",
                order_by
                    .iter()
                    .map(|(expr, order)| format!("{} {}", expr.to_sql_string(context), order))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = &self.limit {
            ret.push(format!("LIMIT {}", limit));
        }
        if let Some(offset) = &self.offset {
            ret.push(format!("OFFSET {}", offset));
        }
        ret.join(" ")
    }
}
