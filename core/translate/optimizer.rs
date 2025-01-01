use std::collections::BTreeSet;
use std::rc::Rc;

use crate::{schema::Index, Result};
use sqlite3_parser::ast;
use sqlite3_parser::ast::{
    FrameBound, FromClause, JoinConstraint, OneSelect, Over, ResultColumn, Select, SelectBody,
    SelectTable, SortedColumn, Window,
};

use super::plan::{
    get_table_ref_bitmask_for_ast_expr, get_table_ref_bitmask_for_operator, Aggregate,
    TableReference, TableReferenceType, ColumnBinding, DeletePlan, Direction, GroupBy, IterationDirection, Plan,
    ResultSetColumn, Search, SelectPlan, SourceOperator,
};

pub fn optimize_plan(plan: &mut Plan) -> Result<()> {
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan),
        Plan::Delete(plan) => optimize_delete_plan(plan),
    }
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
fn optimize_select_plan(plan: &mut SelectPlan) -> Result<()> {
    optimize_subqueries(&mut plan.source)?;
    eliminate_between(&mut plan.source, &mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constants(&mut plan.source, &mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    push_predicates(
        &mut plan.source,
        &mut plan.where_clause,
        &plan.referenced_tables,
    )?;

    use_indexes(
        &mut plan.source,
        &plan.referenced_tables,
        &plan.available_indexes,
    )?;

    eliminate_unnecessary_orderby(
        &mut plan.source,
        &mut plan.order_by,
        &plan.referenced_tables,
        &plan.available_indexes,
    )?;

    related_columns(
        &plan.source,
        &plan.result_columns,
        &plan.where_clause,
        &plan.group_by,
        &plan.order_by,
        &plan.aggregates,
        &mut plan.related_columns,
        &plan.referenced_tables,
    )?;

    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan) -> Result<()> {
    eliminate_between(&mut plan.source, &mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constants(&mut plan.source, &mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    use_indexes(
        &mut plan.source,
        &plan.referenced_tables,
        &plan.available_indexes,
    )?;

    Ok(())
}

fn optimize_subqueries(operator: &mut SourceOperator) -> Result<()> {
    match operator {
        SourceOperator::Subquery { plan, .. } => {
            optimize_select_plan(&mut *plan)?;
            Ok(())
        }
        SourceOperator::Join { left, right, .. } => {
            optimize_subqueries(left)?;
            optimize_subqueries(right)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

fn _operator_is_already_ordered_by(
    operator: &mut SourceOperator,
    key: &mut ast::Expr,
    referenced_tables: &[TableReference],
    available_indexes: &Vec<Rc<Index>>,
) -> Result<bool> {
    match operator {
        SourceOperator::Scan {
            table_reference, ..
        } => Ok(key.is_rowid_alias_of(table_reference.table_index)),
        SourceOperator::Search {
            table_reference,
            search,
            ..
        } => match search {
            Search::RowidEq { .. } => Ok(key.is_rowid_alias_of(table_reference.table_index)),
            Search::RowidSearch { .. } => Ok(key.is_rowid_alias_of(table_reference.table_index)),
            Search::IndexSearch { index, .. } => {
                let index_idx = key.check_index_scan(
                    table_reference.table_index,
                    referenced_tables,
                    available_indexes,
                )?;
                let index_is_the_same = index_idx
                    .map(|i| Rc::ptr_eq(&available_indexes[i], index))
                    .unwrap_or(false);
                Ok(index_is_the_same)
            }
        },
        SourceOperator::Join { left, .. } => {
            _operator_is_already_ordered_by(left, key, referenced_tables, available_indexes)
        }
        _ => Ok(false),
    }
}

fn eliminate_unnecessary_orderby(
    operator: &mut SourceOperator,
    order_by: &mut Option<Vec<(ast::Expr, Direction)>>,
    referenced_tables: &[TableReference],
    available_indexes: &Vec<Rc<Index>>,
) -> Result<()> {
    if order_by.is_none() {
        return Ok(());
    }

    let o = order_by.as_mut().unwrap();

    if o.len() != 1 {
        // TODO: handle multiple order by keys
        return Ok(());
    }

    let (key, direction) = o.first_mut().unwrap();

    let already_ordered =
        _operator_is_already_ordered_by(operator, key, referenced_tables, available_indexes)?;

    if already_ordered {
        push_scan_direction(operator, direction);
        *order_by = None;
    }

    Ok(())
}

/**
 * Use indexes where possible
 */
fn use_indexes(
    operator: &mut SourceOperator,
    referenced_tables: &[TableReference],
    available_indexes: &[Rc<Index>],
) -> Result<()> {
    match operator {
        SourceOperator::Subquery { .. } => Ok(()),
        SourceOperator::Search { .. } => Ok(()),
        SourceOperator::Scan {
            table_reference,
            predicates: filter,
            id,
            ..
        } => {
            if filter.is_none() {
                return Ok(());
            }

            let fs = filter.as_mut().unwrap();
            for i in 0..fs.len() {
                let f = fs[i].take_ownership();
                let table_index = referenced_tables
                    .iter()
                    .position(|t| t.table_identifier == table_reference.table_identifier)
                    .unwrap();
                match try_extract_index_search_expression(
                    f,
                    table_index,
                    referenced_tables,
                    available_indexes,
                )? {
                    Either::Left(non_index_using_expr) => {
                        fs[i] = non_index_using_expr;
                    }
                    Either::Right(index_search) => {
                        fs.remove(i);
                        *operator = SourceOperator::Search {
                            id: *id,
                            table_reference: table_reference.clone(),
                            predicates: Some(fs.clone()),
                            search: index_search,
                        };

                        return Ok(());
                    }
                }
            }

            Ok(())
        }
        SourceOperator::Join { left, right, .. } => {
            use_indexes(left, referenced_tables, available_indexes)?;
            use_indexes(right, referenced_tables, available_indexes)?;
            Ok(())
        }
        SourceOperator::Nothing { .. } => Ok(()),
    }
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

// removes predicates that are always true
// returns a ConstantEliminationResult indicating whether any predicates are always false
fn eliminate_constants(
    operator: &mut SourceOperator,
    where_clause: &mut Option<Vec<ast::Expr>>,
) -> Result<ConstantConditionEliminationResult> {
    if let Some(predicates) = where_clause {
        let mut i = 0;
        while i < predicates.len() {
            let predicate = &predicates[i];
            if predicate.is_always_true()? {
                // true predicates can be removed since they don't affect the result
                predicates.remove(i);
            } else if predicate.is_always_false()? {
                // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false
                predicates.truncate(0);
                return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
            } else {
                i += 1;
            }
        }
    }
    match operator {
        SourceOperator::Subquery { .. } => Ok(ConstantConditionEliminationResult::Continue),
        SourceOperator::Join {
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            if eliminate_constants(left, where_clause)?
                == ConstantConditionEliminationResult::ImpossibleCondition
            {
                return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
            }
            if eliminate_constants(right, where_clause)?
                == ConstantConditionEliminationResult::ImpossibleCondition
                && !*outer
            {
                return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
            }

            if predicates.is_none() {
                return Ok(ConstantConditionEliminationResult::Continue);
            }

            let predicates = predicates.as_mut().unwrap();

            let mut i = 0;
            while i < predicates.len() {
                let predicate = &mut predicates[i];
                if predicate.is_always_true()? {
                    predicates.remove(i);
                } else if predicate.is_always_false()? {
                    if !*outer {
                        predicates.truncate(0);
                        return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
                    }
                    // in an outer join, we can't skip rows, so just replace all constant false predicates with 0
                    // so we don't later have to evaluate anything more complex or special-case the identifiers true and false
                    // which are just aliases for 1 and 0
                    *predicate = ast::Expr::Literal(ast::Literal::Numeric("0".to_string()));
                    i += 1;
                } else {
                    i += 1;
                }
            }

            Ok(ConstantConditionEliminationResult::Continue)
        }
        SourceOperator::Scan { predicates, .. } => {
            if let Some(ps) = predicates {
                let mut i = 0;
                while i < ps.len() {
                    let predicate = &ps[i];
                    if predicate.is_always_true()? {
                        // true predicates can be removed since they don't affect the result
                        ps.remove(i);
                    } else if predicate.is_always_false()? {
                        // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false
                        ps.truncate(0);
                        return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
                    } else {
                        i += 1;
                    }
                }

                if ps.is_empty() {
                    *predicates = None;
                }
            }
            Ok(ConstantConditionEliminationResult::Continue)
        }
        SourceOperator::Search { predicates, .. } => {
            if let Some(predicates) = predicates {
                let mut i = 0;
                while i < predicates.len() {
                    let predicate = &predicates[i];
                    if predicate.is_always_true()? {
                        // true predicates can be removed since they don't affect the result
                        predicates.remove(i);
                    } else if predicate.is_always_false()? {
                        // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false
                        predicates.truncate(0);
                        return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
                    } else {
                        i += 1;
                    }
                }
            }

            Ok(ConstantConditionEliminationResult::Continue)
        }
        SourceOperator::Nothing { .. } => Ok(ConstantConditionEliminationResult::Continue),
    }
}

/**
  Recursively pushes predicates down the tree, as far as possible.
  Where a predicate is pushed determines at which loop level it will be evaluated.
  For example, in SELECT * FROM t1 JOIN t2 JOIN t3 WHERE t1.a = t2.a AND t2.b = t3.b AND t1.c = 1
  the predicate t1.c = 1 can be pushed to t1 and will be evaluated in the first (outermost) loop,
  the predicate t1.a = t2.a can be pushed to t2 and will be evaluated in the second loop
  while t2.b = t3.b will be evaluated in the third loop.
*/
fn push_predicates(
    operator: &mut SourceOperator,
    where_clause: &mut Option<Vec<ast::Expr>>,
    referenced_tables: &Vec<TableReference>,
) -> Result<()> {
    // First try to push down any predicates from the WHERE clause
    if let Some(predicates) = where_clause {
        let mut i = 0;
        while i < predicates.len() {
            // Take ownership of predicate to try pushing it down
            let predicate = predicates[i].take_ownership();
            // If predicate was successfully pushed (None returned), remove it from WHERE
            let Some(predicate) = push_predicate(operator, predicate, referenced_tables)? else {
                predicates.remove(i);
                continue;
            };
            predicates[i] = predicate;
            i += 1;
        }
        // Clean up empty WHERE clause
        if predicates.is_empty() {
            *where_clause = None;
        }
    }

    match operator {
        SourceOperator::Subquery { .. } => Ok(()),
        SourceOperator::Join {
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            // Recursively push predicates down both sides of join
            push_predicates(left, where_clause, referenced_tables)?;
            push_predicates(right, where_clause, referenced_tables)?;

            if predicates.is_none() {
                return Ok(());
            }

            let predicates = predicates.as_mut().unwrap();

            let mut i = 0;
            while i < predicates.len() {
                let predicate_owned = predicates[i].take_ownership();

                // For a join like SELECT * FROM left INNER JOIN right ON left.id = right.id AND left.name = 'foo'
                // the predicate 'left.name = 'foo' can already be evaluated in the outer loop (left side of join)
                // because the row can immediately be skipped if left.name != 'foo'.
                // But for a LEFT JOIN, we can't do this since we need to ensure that all rows from the left table are included,
                // even if there are no matching rows from the right table. This is why we can't push LEFT JOIN predicates to the left side.
                let push_result = if *outer {
                    Some(predicate_owned)
                } else {
                    push_predicate(left, predicate_owned, referenced_tables)?
                };

                // Try pushing to left side first (see comment above for reasoning)
                let Some(predicate) = push_result else {
                    predicates.remove(i);
                    continue;
                };

                // Then try right side
                let Some(predicate) = push_predicate(right, predicate, referenced_tables)? else {
                    predicates.remove(i);
                    continue;
                };

                // If neither side could take it, keep in join predicates (not sure if this actually happens in practice)
                // this is effectively the same as pushing to the right side, so maybe it could be removed and assert here
                // that we don't reach this code
                predicates[i] = predicate;
                i += 1;
            }

            Ok(())
        }
        // Base cases - nowhere else to push to
        SourceOperator::Scan { .. } => Ok(()),
        SourceOperator::Search { .. } => Ok(()),
        SourceOperator::Nothing { .. } => Ok(()),
    }
}

/**
  Push a single predicate down the tree, as far as possible.
  Returns Ok(None) if the predicate was pushed, otherwise returns itself as Ok(Some(predicate))
*/
fn push_predicate(
    operator: &mut SourceOperator,
    predicate: ast::Expr,
    referenced_tables: &Vec<TableReference>,
) -> Result<Option<ast::Expr>> {
    match operator {
        SourceOperator::Subquery {
            predicates,
            table_reference,
            ..
        } => {
            // **TODO**: we are currently just evaluating the predicate after the subquery yields,
            // and not trying to do anythign more sophisticated.
            // E.g. literally: SELECT * FROM (SELECT * FROM t1) sub WHERE sub.col = 'foo'
            //
            // It is possible, and not overly difficult, to determine that we can also push the
            // predicate into the subquery coroutine itself before it yields. The above query would
            // effectively become: SELECT * FROM (SELECT * FROM t1 WHERE col = 'foo') sub
            //
            // This matters more in cases where the subquery builds some kind of sorter/index in memory
            // (or on disk) and in those cases pushing the predicate down to the coroutine will make the
            // subquery produce less intermediate data. In cases where no intermediate data structures are
            // built, it doesn't matter.
            //
            // Moreover, in many cases the subquery can even be completely eliminated, e.g. the above original
            // query would become: SELECT * FROM t1 WHERE col = 'foo' without the subquery.
            // **END TODO**

            // Find position of this subquery in referenced_tables array
            let subquery_index = referenced_tables
                .iter()
                .position(|t| {
                    t.table_identifier == table_reference.table_identifier
                        && matches!(t.reference_type, TableReferenceType::Subquery { .. })
                })
                .unwrap();

            // Get bitmask showing which tables this predicate references
            let predicate_bitmask =
                get_table_ref_bitmask_for_ast_expr(referenced_tables, &predicate)?;

            // Each table has a bit position based on join order from left to right
            // e.g. in SELECT * FROM t1 JOIN t2 JOIN t3
            // t1 is position 0 (001), t2 is position 1 (010), t3 is position 2 (100)
            // To push a predicate to a given table, it can only reference that table and tables to its left
            // Example: For table t2 at position 1 (bit 010):
            // - Can push: 011 (t2 + t1), 001 (just t1), 010 (just t2)
            // - Can't push: 110 (t2 + t3)
            let next_table_on_the_right_in_join_bitmask = 1 << (subquery_index + 1);
            if predicate_bitmask >= next_table_on_the_right_in_join_bitmask {
                return Ok(Some(predicate));
            }

            if predicates.is_none() {
                predicates.replace(vec![predicate]);
            } else {
                predicates.as_mut().unwrap().push(predicate);
            }

            Ok(None)
        }
        SourceOperator::Scan {
            predicates,
            table_reference,
            ..
        } => {
            // Find position of this table in referenced_tables array
            let table_index = referenced_tables
                .iter()
                .position(|t| {
                    t.table_identifier == table_reference.table_identifier
                        && t.reference_type == TableReferenceType::BTreeTable
                })
                .unwrap();

            // Get bitmask showing which tables this predicate references
            let predicate_bitmask =
                get_table_ref_bitmask_for_ast_expr(referenced_tables, &predicate)?;

            // Each table has a bit position based on join order from left to right
            // e.g. in SELECT * FROM t1 JOIN t2 JOIN t3
            // t1 is position 0 (001), t2 is position 1 (010), t3 is position 2 (100)
            // To push a predicate to a given table, it can only reference that table and tables to its left
            // Example: For table t2 at position 1 (bit 010):
            // - Can push: 011 (t2 + t1), 001 (just t1), 010 (just t2)
            // - Can't push: 110 (t2 + t3)
            let next_table_on_the_right_in_join_bitmask = 1 << (table_index + 1);
            if predicate_bitmask >= next_table_on_the_right_in_join_bitmask {
                return Ok(Some(predicate));
            }

            // Add predicate to this table's filters
            if predicates.is_none() {
                predicates.replace(vec![predicate]);
            } else {
                predicates.as_mut().unwrap().push(predicate);
            }

            Ok(None)
        }
        // Search nodes don't exist yet at this point; Scans are transformed to Search in use_indexes()
        SourceOperator::Search { .. } => unreachable!(),
        SourceOperator::Join {
            left,
            right,
            predicates: join_on_preds,
            outer,
            ..
        } => {
            // Try pushing to left side first
            let push_result_left = push_predicate(left, predicate, referenced_tables)?;
            if push_result_left.is_none() {
                return Ok(None);
            }
            // Then try right side
            let push_result_right =
                push_predicate(right, push_result_left.unwrap(), referenced_tables)?;
            if push_result_right.is_none() {
                return Ok(None);
            }

            // For LEFT JOIN, predicates must stay at join level
            if *outer {
                return Ok(Some(push_result_right.unwrap()));
            }

            let pred = push_result_right.unwrap();

            // Get bitmasks for tables referenced in predicate and both sides of join
            let table_refs_bitmask = get_table_ref_bitmask_for_ast_expr(referenced_tables, &pred)?;
            let left_bitmask = get_table_ref_bitmask_for_operator(referenced_tables, left)?;
            let right_bitmask = get_table_ref_bitmask_for_operator(referenced_tables, right)?;

            // If predicate doesn't reference tables from both sides, it can't be a join condition
            if table_refs_bitmask & left_bitmask == 0 || table_refs_bitmask & right_bitmask == 0 {
                return Ok(Some(pred));
            }

            // Add as join predicate since it references both sides
            if join_on_preds.is_none() {
                join_on_preds.replace(vec![pred]);
            } else {
                join_on_preds.as_mut().unwrap().push(pred);
            }

            Ok(None)
        }
        SourceOperator::Nothing { .. } => Ok(Some(predicate)),
    }
}

fn push_scan_direction(operator: &mut SourceOperator, direction: &Direction) {
    match operator {
        SourceOperator::Scan { iter_dir, .. } => {
            if iter_dir.is_none() {
                match direction {
                    Direction::Ascending => *iter_dir = Some(IterationDirection::Forwards),
                    Direction::Descending => *iter_dir = Some(IterationDirection::Backwards),
                }
            }
        }
        _ => todo!(),
    }
}

fn eliminate_between(
    operator: &mut SourceOperator,
    where_clauses: &mut Option<Vec<ast::Expr>>,
) -> Result<()> {
    if let Some(predicates) = where_clauses {
        *predicates = predicates.drain(..).map(convert_between_expr).collect();
    }

    match operator {
        SourceOperator::Join {
            left,
            right,
            predicates,
            ..
        } => {
            eliminate_between(left, where_clauses)?;
            eliminate_between(right, where_clauses)?;

            if let Some(predicates) = predicates {
                *predicates = predicates.drain(..).map(convert_between_expr).collect();
            }
        }
        SourceOperator::Scan {
            predicates: Some(preds),
            ..
        } => {
            *preds = preds.drain(..).map(convert_between_expr).collect();
        }
        SourceOperator::Search {
            predicates: Some(preds),
            ..
        } => {
            *preds = preds.drain(..).map(convert_between_expr).collect();
        }
        _ => (),
    }

    Ok(())
}

fn related_columns(
    operator: &SourceOperator,
    result_columns: &[ResultSetColumn],
    where_clause: &Option<Vec<ast::Expr>>,
    group_by: &Option<GroupBy>,
    order_by: &Option<Vec<(ast::Expr, Direction)>>,
    aggregates: &[Aggregate],
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    source_related_columns(operator, related_columns, referenced_tables)?;
    for column in result_columns.iter() {
        expr_related_columns(&column.expr, related_columns, referenced_tables)?;
    }
    if let Some(exprs) = where_clause {
        for expr in exprs {
            expr_related_columns(expr, related_columns, referenced_tables)?;
        }
    }
    if let Some(group_by) = group_by {
        for expr in group_by.exprs.iter() {
            expr_related_columns(expr, related_columns, referenced_tables)?;
        }

        if let Some(exprs) = &group_by.having {
            for expr in exprs {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
        }
    }
    if let Some(order_by) = order_by {
        for (expr, _) in order_by {
            expr_related_columns(expr, related_columns, referenced_tables)?;
        }
    }
    for agg in aggregates {
        for expr in agg.args.iter() {
            expr_related_columns(expr, related_columns, referenced_tables)?;
        }
        expr_related_columns(&agg.original_expr, related_columns, referenced_tables)?;
    }

    Ok(())
}

fn source_related_columns(
    source_operator: &SourceOperator,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match source_operator {
        SourceOperator::Join {
            left,
            right,
            predicates,
            ..
        } => {
            source_related_columns(left, related_columns, referenced_tables)?;
            source_related_columns(right, related_columns, referenced_tables)?;

            if let Some(exprs) = predicates {
                for expr in exprs.iter() {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
        }
        SourceOperator::Scan { predicates, .. } => {
            if let Some(predicates) = predicates {
                for expr in predicates {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
        }
        SourceOperator::Search {
            predicates, search, ..
        } => {
            if let Some(exprs) = predicates {
                for expr in exprs.iter() {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
            match search {
                Search::RowidEq { cmp_expr }
                | Search::RowidSearch { cmp_expr, .. }
                | Search::IndexSearch { cmp_expr, .. } => {
                    expr_related_columns(cmp_expr, related_columns, referenced_tables)?;
                }
            }
        }
        SourceOperator::Nothing => (),
    }

    Ok(())
}

fn from_clause_related_columns(
    from: &FromClause,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    if let Some(select_table) = &from.select {
        select_table_related_columns(select_table, related_columns, referenced_tables)?;
    }
    if let Some(joins) = &from.joins {
        for join in joins.iter() {
            select_table_related_columns(&join.table, related_columns, referenced_tables)?;

            if let Some(constraint) = &join.constraint {
                match constraint {
                    JoinConstraint::On(expr) => {
                        expr_related_columns(expr, related_columns, referenced_tables)?;
                    }
                    JoinConstraint::Using(_) => (),
                }
            }
        }
    }

    Ok(())
}

fn select_table_related_columns(
    select_table: &SelectTable,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match select_table {
        SelectTable::TableCall(_, exprs, _) => {
            if let Some(exprs) = exprs {
                for expr in exprs {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
        }
        SelectTable::Select(select, _) => {
            select_related_columns(select, related_columns, referenced_tables)?;
        }
        SelectTable::Sub(from, _) => {
            from_clause_related_columns(from, related_columns, referenced_tables)?;
        }
        SelectTable::Table(_, _, _) => (),
    }

    Ok(())
}

fn one_select_related_columns(
    one_select: &OneSelect,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match one_select {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            for column in columns {
                match &column {
                    ResultColumn::Expr(expr, _) => {
                        expr_related_columns(expr, related_columns, referenced_tables)?;
                    }
                    ResultColumn::Star => {
                        full_related_columns(related_columns, referenced_tables);
                    }
                    ResultColumn::TableStar(table_name) => {
                        let Some(table_pos) = referenced_tables
                            .iter()
                            .position(|table| table.table.name == table_name.0)
                        else {
                            crate::bail_corrupt_error!(
                                "Optimize error: no such table: {}",
                                table_name
                            )
                        };
                        for i in 0..referenced_tables[table_pos].table.columns.len() {
                            related_columns.insert(ColumnBinding {
                                table: table_pos,
                                column: i,
                            });
                        }
                    }
                }
            }
            if let Some(from) = from {
                from_clause_related_columns(from, related_columns, referenced_tables)?;
            }
            if let Some(expr) = where_clause {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
            if let Some(group_by) = group_by {
                for expr in group_by.exprs.iter() {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
                if let Some(expr) = &group_by.having {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
            if let Some(windows) = window_clause {
                for window in windows.iter().map(|window| &window.window) {
                    window_related_columns(window, related_columns, referenced_tables)?;
                }
            }
        }
        OneSelect::Values(rows) => {
            for row in rows.iter() {
                for expr in row {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
        }
    }

    Ok(())
}

fn select_related_columns(
    select: &Select,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    let Select {
        body: SelectBody { select, compounds },
        order_by,
        limit,
        ..
    } = &select;

    one_select_related_columns(select, related_columns, referenced_tables)?;

    if let Some(compounds) = compounds {
        for compound in compounds {
            one_select_related_columns(&compound.select, related_columns, referenced_tables)?;
        }
    }
    order_by_related_columns(order_by, related_columns, referenced_tables)?;
    if let Some(limit) = limit {
        expr_related_columns(&limit.expr, related_columns, referenced_tables)?;
        if let Some(offset) = &limit.offset {
            expr_related_columns(offset, related_columns, referenced_tables)?;
        }
    }

    Ok(())
}

fn expr_related_columns(
    expr: &ast::Expr,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match expr {
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            expr_related_columns(lhs, related_columns, referenced_tables)?;
            expr_related_columns(start, related_columns, referenced_tables)?;
            expr_related_columns(end, related_columns, referenced_tables)?;
        }
        ast::Expr::Binary(lhs, _, rhs) => {
            expr_related_columns(lhs, related_columns, referenced_tables)?;
            expr_related_columns(rhs, related_columns, referenced_tables)?;
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(expr) = base {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
            for (lhs, rhs) in when_then_pairs {
                expr_related_columns(lhs, related_columns, referenced_tables)?;
                expr_related_columns(rhs, related_columns, referenced_tables)?;
            }
            if let Some(expr) = else_expr {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
        }
        ast::Expr::Cast { expr, .. }
        | ast::Expr::Collate(expr, _)
        | ast::Expr::IsNull(expr)
        | ast::Expr::NotNull(expr)
        | ast::Expr::Unary(_, expr) => {
            expr_related_columns(expr, related_columns, referenced_tables)?;
        }
        ast::Expr::Exists(select) => {
            select_related_columns(select, related_columns, referenced_tables)?;
        }
        ast::Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            if let Some(exprs) = args {
                for expr in exprs {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
            order_by_related_columns(order_by, related_columns, referenced_tables)?;

            if let Some(filter_over) = filter_over {
                if let Some(over) = &filter_over.over_clause {
                    match over.as_ref() {
                        Over::Window(window) => {
                            window_related_columns(window, related_columns, referenced_tables)?;
                        }
                        Over::Name(_) => (),
                    }
                }
                if let Some(expr) = &filter_over.filter_clause {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
        }
        ast::Expr::FunctionCallStar { .. } => {
            full_related_columns(related_columns, referenced_tables);
        }
        ast::Expr::Column { table, column, .. } => {
            related_columns.insert(ColumnBinding {
                table: *table,
                column: *column,
            });
        }
        ast::Expr::InList {
            lhs, rhs: exprs, ..
        }
        | ast::Expr::InTable {
            lhs, args: exprs, ..
        } => {
            expr_related_columns(lhs, related_columns, referenced_tables)?;
            if let Some(exprs) = exprs {
                for expr in exprs {
                    expr_related_columns(expr, related_columns, referenced_tables)?;
                }
            }
        }
        ast::Expr::InSelect { lhs, rhs, .. } => {
            expr_related_columns(lhs, related_columns, referenced_tables)?;
            select_related_columns(rhs, related_columns, referenced_tables)?;
        }
        ast::Expr::Like {
            lhs, rhs, escape, ..
        } => {
            expr_related_columns(lhs, related_columns, referenced_tables)?;
            expr_related_columns(rhs, related_columns, referenced_tables)?;
            if let Some(expr) = escape {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            for expr in exprs {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
        }
        ast::Expr::Raise(_, expr) => {
            if let Some(expr) = expr {
                expr_related_columns(expr, related_columns, referenced_tables)?;
            }
        }
        ast::Expr::Subquery(select) => {
            select_related_columns(select, related_columns, referenced_tables)?;
        }

        ast::Expr::Id(_)
        | ast::Expr::DoublyQualified(_, _, _)
        | ast::Expr::Literal(_)
        | ast::Expr::Name(_)
        | ast::Expr::Qualified(_, _)
        | ast::Expr::Variable(_) => (),
    }

    Ok(())
}

#[inline]
fn window_related_columns(
    window: &Window,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    if let Some(partition_by) = &window.partition_by {
        for expr in partition_by {
            expr_related_columns(expr, related_columns, referenced_tables)?;
        }
    }
    order_by_related_columns(&window.order_by, related_columns, referenced_tables)?;
    if let Some(frame) = &window.frame_clause {
        let mut fn_bound = |bound: &FrameBound| match bound {
            FrameBound::Following(expr) | FrameBound::Preceding(expr) => {
                expr_related_columns(expr, related_columns, referenced_tables)
            }
            FrameBound::CurrentRow
            | FrameBound::UnboundedFollowing
            | FrameBound::UnboundedPreceding => Ok(()),
        };
        fn_bound(&frame.start)?;

        if let Some(end) = &frame.end {
            fn_bound(end)?;
        }
    }
    Ok(())
}

#[inline]
fn full_related_columns(
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) {
    for (table_pos, reference_table) in referenced_tables.iter().enumerate() {
        for i in 0..reference_table.table.columns.len() {
            related_columns.insert(ColumnBinding {
                table: table_pos,
                column: i,
            });
        }
    }
}

#[inline]
fn order_by_related_columns(
    order_by: &Option<Vec<SortedColumn>>,
    related_columns: &mut BTreeSet<ColumnBinding>,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    if let Some(sort_columns) = order_by {
        for column in sort_columns {
            expr_related_columns(&column.expr, related_columns, referenced_tables)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstantPredicate {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression e.g. '1', returns the constant condition
    fn check_constant(&self) -> Result<Option<ConstantPredicate>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantPredicate::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantPredicate::AlwaysFalse))
    }
    fn is_rowid_alias_of(&self, table_index: usize) -> bool;
    fn check_index_scan(
        &mut self,
        table_index: usize,
        referenced_tables: &[TableReference],
        available_indexes: &[Rc<Index>],
    ) -> Result<Option<usize>>;
}

impl Optimizable for ast::Expr {
    fn is_rowid_alias_of(&self, table_index: usize) -> bool {
        match self {
            Self::Column {
                table,
                is_rowid_alias,
                ..
            } => *is_rowid_alias && *table == table_index,
            _ => false,
        }
    }
    fn check_index_scan(
        &mut self,
        table_index: usize,
        referenced_tables: &[TableReference],
        available_indexes: &[Rc<Index>],
    ) -> Result<Option<usize>> {
        match self {
            Self::Column { table, column, .. } => {
                if *table != table_index {
                    return Ok(None);
                }
                for (idx, index) in available_indexes.iter().enumerate() {
                    let table_ref = &referenced_tables[*table];
                    if index.table_name == table_ref.table.get_name() {
                        let column = table_ref.table.get_column_at(*column);
                        if index.columns.first().unwrap().name == column.name {
                            return Ok(Some(idx));
                        }
                    }
                }
                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_index =
                    lhs.check_index_scan(table_index, referenced_tables, available_indexes)?;
                if lhs_index.is_some() {
                    return Ok(lhs_index);
                }
                let rhs_index =
                    rhs.check_index_scan(table_index, referenced_tables, available_indexes)?;
                if rhs_index.is_some() {
                    // swap lhs and rhs
                    let lhs_new = rhs.take_ownership();
                    let rhs_new = lhs.take_ownership();
                    *self = Self::Binary(Box::new(lhs_new), *op, Box::new(rhs_new));
                    return Ok(rhs_index);
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
    fn check_constant(&self) -> Result<Option<ConstantPredicate>> {
        match self {
            Self::Id(id) => {
                // true and false are special constants that are effectively aliases for 1 and 0
                if id.0.eq_ignore_ascii_case("true") {
                    return Ok(Some(ConstantPredicate::AlwaysTrue));
                }
                if id.0.eq_ignore_ascii_case("false") {
                    return Ok(Some(ConstantPredicate::AlwaysFalse));
                }
                Ok(None)
            }
            Self::Literal(lit) => match lit {
                ast::Literal::Null => Ok(Some(ConstantPredicate::AlwaysFalse)),
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    Ok(Some(ConstantPredicate::AlwaysFalse))
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial.map(|t| match t {
                        ConstantPredicate::AlwaysTrue => ConstantPredicate::AlwaysFalse,
                        ConstantPredicate::AlwaysFalse => ConstantPredicate::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        ConstantPredicate::AlwaysTrue
                    } else {
                        ConstantPredicate::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        ConstantPredicate::AlwaysTrue
                    } else {
                        ConstantPredicate::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_constant()?;
                let rhs_trivial = rhs.check_constant()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                            || rhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysFalse));
                        }
                        if lhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                            && rhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                            || rhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysTrue));
                        }
                        if lhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                            && rhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

pub enum Either<T, U> {
    Left(T),
    Right(U),
}

pub fn try_extract_index_search_expression(
    expr: ast::Expr,
    table_index: usize,
    referenced_tables: &[TableReference],
    available_indexes: &[Rc<Index>],
) -> Result<Either<ast::Expr, Search>> {
    match expr {
        ast::Expr::Binary(mut lhs, operator, mut rhs) => {
            if lhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        return Ok(Either::Right(Search::RowidEq { cmp_expr: *rhs }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::RowidSearch {
                            cmp_op: operator,
                            cmp_expr: *rhs,
                        }));
                    }
                    _ => {}
                }
            }

            if rhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        return Ok(Either::Right(Search::RowidEq { cmp_expr: *lhs }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::RowidSearch {
                            cmp_op: operator,
                            cmp_expr: *lhs,
                        }));
                    }
                    _ => {}
                }
            }

            if let Some(index_index) =
                lhs.check_index_scan(table_index, referenced_tables, available_indexes)?
            {
                match operator {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::IndexSearch {
                            index: available_indexes[index_index].clone(),
                            cmp_op: operator,
                            cmp_expr: *rhs,
                        }));
                    }
                    _ => {}
                }
            }

            if let Some(index_index) =
                rhs.check_index_scan(table_index, referenced_tables, available_indexes)?
            {
                match operator {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::IndexSearch {
                            index: available_indexes[index_index].clone(),
                            cmp_op: operator,
                            cmp_expr: *lhs,
                        }));
                    }
                    _ => {}
                }
            }

            Ok(Either::Left(ast::Expr::Binary(lhs, operator, rhs)))
        }
        _ => Ok(Either::Left(expr)),
    }
}

fn convert_between_expr(expr: ast::Expr) -> ast::Expr {
    match expr {
        ast::Expr::Between {
            lhs,
            not,
            start,
            end,
        } => {
            // Convert `y NOT BETWEEN x AND z` to `x > y OR y > z`
            let (lower_op, upper_op) = if not {
                (ast::Operator::Greater, ast::Operator::Greater)
            } else {
                // Convert `y BETWEEN x AND z` to `x <= y AND y <= z`
                (ast::Operator::LessEquals, ast::Operator::LessEquals)
            };

            let lower_bound = ast::Expr::Binary(start, lower_op, lhs.clone());
            let upper_bound = ast::Expr::Binary(lhs, upper_op, end);

            if not {
                ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::Or,
                    Box::new(upper_bound),
                )
            } else {
                ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::And,
                    Box::new(upper_bound),
                )
            }
        }
        ast::Expr::Parenthesized(mut exprs) => {
            ast::Expr::Parenthesized(exprs.drain(..).map(convert_between_expr).collect())
        }
        // Process other expressions recursively
        ast::Expr::Binary(lhs, op, rhs) => ast::Expr::Binary(
            Box::new(convert_between_expr(*lhs)),
            op,
            Box::new(convert_between_expr(*rhs)),
        ),
        ast::Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            filter_over,
        } => ast::Expr::FunctionCall {
            name,
            distinctness,
            args: args.map(|args| args.into_iter().map(convert_between_expr).collect()),
            order_by,
            filter_over,
        },
        _ => expr,
    }
}

trait TakeOwnership {
    fn take_ownership(&mut self) -> Self;
}

impl TakeOwnership for ast::Expr {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, ast::Expr::Literal(ast::Literal::Null))
    }
}
