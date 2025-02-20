use core::num::IntErrorKind;
use limbo_sqlite3_parser::ast::{self, CreateTableBody, Expr, FunctionTail, Literal};
use std::{rc::Rc, sync::Arc};

use crate::{
    schema::{self, Column, Schema, Type},
    types::OwnedValue,
    Result, Statement, StepResult, IO,
};

// https://sqlite.org/lang_keywords.html
const QUOTE_PAIRS: &[(char, char)] = &[('"', '"'), ('[', ']'), ('`', '`')];

pub fn normalize_ident(identifier: &str) -> String {
    let quote_pair = QUOTE_PAIRS
        .iter()
        .find(|&(start, end)| identifier.starts_with(*start) && identifier.ends_with(*end));

    if let Some(&(_, _)) = quote_pair {
        &identifier[1..identifier.len() - 1]
    } else {
        identifier
    }
    .to_lowercase()
}

pub const PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX: &str = "sqlite_autoindex_";

pub fn parse_schema_rows(
    rows: Option<Statement>,
    schema: &mut Schema,
    io: Arc<dyn IO>,
) -> Result<()> {
    if let Some(mut rows) = rows {
        let mut automatic_indexes = Vec::new();
        loop {
            match rows.step()? {
                StepResult::Row => {
                    let row = rows.row().unwrap();
                    let ty = row.get::<&str>(0)?;
                    if ty != "table" && ty != "index" {
                        continue;
                    }
                    match ty {
                        "table" => {
                            let root_page: i64 = row.get::<i64>(3)?;
                            let sql: &str = row.get::<&str>(4)?;
                            let table = schema::BTreeTable::from_sql(sql, root_page as usize)?;
                            schema.add_table(Rc::new(table));
                        }
                        "index" => {
                            let root_page: i64 = row.get::<i64>(3)?;
                            match row.get::<&str>(4) {
                                Ok(sql) => {
                                    let index = schema::Index::from_sql(sql, root_page as usize)?;
                                    schema.add_index(Rc::new(index));
                                }
                                _ => {
                                    // Automatic index on primary key, e.g.
                                    // table|foo|foo|2|CREATE TABLE foo (a text PRIMARY KEY, b)
                                    // index|sqlite_autoindex_foo_1|foo|3|
                                    let index_name = row.get::<&str>(1)?;
                                    let table_name = row.get::<&str>(2)?;
                                    let root_page = row.get::<i64>(3)?;
                                    automatic_indexes.push((
                                        index_name.to_string(),
                                        table_name.to_string(),
                                        root_page,
                                    ));
                                }
                            }
                        }
                        _ => continue,
                    }
                }
                StepResult::IO => {
                    // TODO: How do we ensure that the I/O we submitted to
                    // read the schema is actually complete?
                    io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => break,
            }
        }
        for (index_name, table_name, root_page) in automatic_indexes {
            // We need to process these after all tables are loaded into memory due to the schema.get_table() call
            let table = schema.get_table(&table_name).unwrap();
            let index =
                schema::Index::automatic_from_primary_key(&table, &index_name, root_page as usize)?;
            schema.add_index(Rc::new(index));
        }
    }
    Ok(())
}

fn cmp_numeric_strings(num_str: &str, other: &str) -> bool {
    match (num_str.parse::<f64>(), other.parse::<f64>()) {
        (Ok(num), Ok(other)) => num == other,
        _ => num_str == other,
    }
}

pub fn check_ident_equivalency(ident1: &str, ident2: &str) -> bool {
    fn strip_quotes(identifier: &str) -> &str {
        for &(start, end) in QUOTE_PAIRS {
            if identifier.starts_with(start) && identifier.ends_with(end) {
                return &identifier[1..identifier.len() - 1];
            }
        }
        identifier
    }
    strip_quotes(ident1).eq_ignore_ascii_case(strip_quotes(ident2))
}

pub fn check_literal_equivalency(lhs: &Literal, rhs: &Literal) -> bool {
    match (lhs, rhs) {
        (Literal::Numeric(n1), Literal::Numeric(n2)) => cmp_numeric_strings(n1, n2),
        (Literal::String(s1), Literal::String(s2)) => check_ident_equivalency(s1, s2),
        (Literal::Blob(b1), Literal::Blob(b2)) => b1 == b2,
        (Literal::Keyword(k1), Literal::Keyword(k2)) => check_ident_equivalency(k1, k2),
        (Literal::Null, Literal::Null) => true,
        (Literal::CurrentDate, Literal::CurrentDate) => true,
        (Literal::CurrentTime, Literal::CurrentTime) => true,
        (Literal::CurrentTimestamp, Literal::CurrentTimestamp) => true,
        _ => false,
    }
}

/// This function is used to determine whether two expressions are logically
/// equivalent in the context of queries, even if their representations
/// differ. e.g.: `SUM(x)` and `sum(x)`, `x + y` and `y + x`
///
/// *Note*: doesn't attempt to evaluate/compute "constexpr" results
pub fn exprs_are_equivalent(expr1: &Expr, expr2: &Expr) -> bool {
    match (expr1, expr2) {
        (
            Expr::Between {
                lhs: lhs1,
                not: not1,
                start: start1,
                end: end1,
            },
            Expr::Between {
                lhs: lhs2,
                not: not2,
                start: start2,
                end: end2,
            },
        ) => {
            not1 == not2
                && exprs_are_equivalent(lhs1, lhs2)
                && exprs_are_equivalent(start1, start2)
                && exprs_are_equivalent(end1, end2)
        }
        (Expr::Binary(lhs1, op1, rhs1), Expr::Binary(lhs2, op2, rhs2)) => {
            op1 == op2
                && ((exprs_are_equivalent(lhs1, lhs2) && exprs_are_equivalent(rhs1, rhs2))
                    || (op1.is_commutative()
                        && exprs_are_equivalent(lhs1, rhs2)
                        && exprs_are_equivalent(rhs1, lhs2)))
        }
        (
            Expr::Case {
                base: base1,
                when_then_pairs: pairs1,
                else_expr: else1,
            },
            Expr::Case {
                base: base2,
                when_then_pairs: pairs2,
                else_expr: else2,
            },
        ) => {
            base1 == base2
                && pairs1.len() == pairs2.len()
                && pairs1.iter().zip(pairs2).all(|((w1, t1), (w2, t2))| {
                    exprs_are_equivalent(w1, w2) && exprs_are_equivalent(t1, t2)
                })
                && else1 == else2
        }
        (
            Expr::Cast {
                expr: expr1,
                type_name: type1,
            },
            Expr::Cast {
                expr: expr2,
                type_name: type2,
            },
        ) => {
            exprs_are_equivalent(expr1, expr2)
                && match (type1, type2) {
                    (Some(t1), Some(t2)) => t1.name.eq_ignore_ascii_case(&t2.name),
                    _ => false,
                }
        }
        (Expr::Collate(expr1, collation1), Expr::Collate(expr2, collation2)) => {
            exprs_are_equivalent(expr1, expr2) && collation1.eq_ignore_ascii_case(collation2)
        }
        (
            Expr::FunctionCall {
                name: name1,
                distinctness: distinct1,
                args: args1,
                order_by: order1,
                filter_over: filter1,
            },
            Expr::FunctionCall {
                name: name2,
                distinctness: distinct2,
                args: args2,
                order_by: order2,
                filter_over: filter2,
            },
        ) => {
            name1.0.eq_ignore_ascii_case(&name2.0)
                && distinct1 == distinct2
                && args1 == args2
                && order1 == order2
                && filter1 == filter2
        }
        (
            Expr::FunctionCallStar {
                name: name1,
                filter_over: filter1,
            },
            Expr::FunctionCallStar {
                name: name2,
                filter_over: filter2,
            },
        ) => {
            name1.0.eq_ignore_ascii_case(&name2.0)
                && match (filter1, filter2) {
                    (None, None) => true,
                    (
                        Some(FunctionTail {
                            filter_clause: fc1,
                            over_clause: oc1,
                        }),
                        Some(FunctionTail {
                            filter_clause: fc2,
                            over_clause: oc2,
                        }),
                    ) => match ((fc1, fc2), (oc1, oc2)) {
                        ((Some(fc1), Some(fc2)), (Some(oc1), Some(oc2))) => {
                            exprs_are_equivalent(fc1, fc2) && oc1 == oc2
                        }
                        ((Some(fc1), Some(fc2)), _) => exprs_are_equivalent(fc1, fc2),
                        _ => false,
                    },
                    _ => false,
                }
        }
        (Expr::NotNull(expr1), Expr::NotNull(expr2)) => exprs_are_equivalent(expr1, expr2),
        (Expr::IsNull(expr1), Expr::IsNull(expr2)) => exprs_are_equivalent(expr1, expr2),
        (Expr::Literal(lit1), Expr::Literal(lit2)) => check_literal_equivalency(lit1, lit2),
        (Expr::Id(id1), Expr::Id(id2)) => check_ident_equivalency(&id1.0, &id2.0),
        (Expr::Unary(op1, expr1), Expr::Unary(op2, expr2)) => {
            op1 == op2 && exprs_are_equivalent(expr1, expr2)
        }
        (Expr::Variable(var1), Expr::Variable(var2)) => var1 == var2,
        (Expr::Parenthesized(exprs1), Expr::Parenthesized(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1
                    .iter()
                    .zip(exprs2)
                    .all(|(e1, e2)| exprs_are_equivalent(e1, e2))
        }
        (Expr::Parenthesized(exprs1), exprs2) | (exprs2, Expr::Parenthesized(exprs1)) => {
            exprs1.len() == 1 && exprs_are_equivalent(&exprs1[0], exprs2)
        }
        (Expr::Qualified(tn1, cn1), Expr::Qualified(tn2, cn2)) => {
            check_ident_equivalency(&tn1.0, &tn2.0) && check_ident_equivalency(&cn1.0, &cn2.0)
        }
        (Expr::DoublyQualified(sn1, tn1, cn1), Expr::DoublyQualified(sn2, tn2, cn2)) => {
            check_ident_equivalency(&sn1.0, &sn2.0)
                && check_ident_equivalency(&tn1.0, &tn2.0)
                && check_ident_equivalency(&cn1.0, &cn2.0)
        }
        (
            Expr::InList {
                lhs: lhs1,
                not: not1,
                rhs: rhs1,
            },
            Expr::InList {
                lhs: lhs2,
                not: not2,
                rhs: rhs2,
            },
        ) => {
            *not1 == *not2
                && exprs_are_equivalent(lhs1, lhs2)
                && rhs1
                    .as_ref()
                    .zip(rhs2.as_ref())
                    .map(|(list1, list2)| {
                        list1.len() == list2.len()
                            && list1
                                .iter()
                                .zip(list2)
                                .all(|(e1, e2)| exprs_are_equivalent(e1, e2))
                    })
                    .unwrap_or(false)
        }
        // fall back to naive equality check
        _ => expr1 == expr2,
    }
}

pub fn columns_from_create_table_body(body: ast::CreateTableBody) -> Result<Vec<Column>, ()> {
    let CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Err(());
    };

    Ok(columns
        .into_iter()
        .filter_map(|(name, column_def)| {
            // if column_def.col_type includes HIDDEN, omit it for now
            if let Some(data_type) = column_def.col_type.as_ref() {
                if data_type.name.as_str().contains("HIDDEN") {
                    return None;
                }
            }
            let column = Column {
                name: Some(name.0),
                ty: match column_def.col_type {
                    Some(ref data_type) => {
                        // https://www.sqlite.org/datatype3.html
                        let type_name = data_type.name.as_str().to_uppercase();
                        if type_name.contains("INT") {
                            Type::Integer
                        } else if type_name.contains("CHAR")
                            || type_name.contains("CLOB")
                            || type_name.contains("TEXT")
                        {
                            Type::Text
                        } else if type_name.contains("BLOB") || type_name.is_empty() {
                            Type::Blob
                        } else if type_name.contains("REAL")
                            || type_name.contains("FLOA")
                            || type_name.contains("DOUB")
                        {
                            Type::Real
                        } else {
                            Type::Numeric
                        }
                    }
                    None => Type::Null,
                },
                default: column_def
                    .constraints
                    .iter()
                    .find_map(|c| match &c.constraint {
                        limbo_sqlite3_parser::ast::ColumnConstraint::Default(val) => {
                            Some(val.clone())
                        }
                        _ => None,
                    }),
                notnull: column_def.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        limbo_sqlite3_parser::ast::ColumnConstraint::NotNull { .. }
                    )
                }),
                ty_str: column_def
                    .col_type
                    .clone()
                    .map(|t| t.name.to_string())
                    .unwrap_or_default(),
                primary_key: column_def.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        limbo_sqlite3_parser::ast::ColumnConstraint::PrimaryKey { .. }
                    )
                }),
                is_rowid_alias: false,
            };
            Some(column)
        })
        .collect::<Vec<_>>())
}

#[derive(Debug, PartialEq)]
/// Reference:
/// https://github.com/sqlite/sqlite/blob/master/src/util.c#L798
pub enum CastTextToIntResultCode {
    NotInt = -1,
    Success = 0,
    ExcessSpace = 1,
    TooLargeOrMalformed = 2,
    #[allow(dead_code)]
    SpecialCase = 3,
}

pub fn text_to_integer(text: &str) -> (OwnedValue, CastTextToIntResultCode) {
    let text = text.trim();
    if text.is_empty() {
        return (OwnedValue::Integer(0), CastTextToIntResultCode::NotInt);
    }
    let mut accum = String::new();
    let mut sign = false;
    let mut has_digit = false;
    let mut excess_space = false;

    let chars = text.chars();

    for c in chars {
        match c {
            '0'..='9' => {
                has_digit = true;
                accum.push(c);
            }
            '+' | '-' if !has_digit && !sign => {
                sign = true;
                accum.push(c);
            }
            _ => {
                excess_space = true;
                break;
            }
        }
    }

    match accum.parse::<i64>() {
        Ok(num) => {
            if excess_space {
                return (
                    OwnedValue::Integer(num),
                    CastTextToIntResultCode::ExcessSpace,
                );
            }

            return (OwnedValue::Integer(num), CastTextToIntResultCode::Success);
        }
        Err(e) => match e.kind() {
            IntErrorKind::NegOverflow | IntErrorKind::PosOverflow => (
                OwnedValue::Integer(0),
                CastTextToIntResultCode::TooLargeOrMalformed,
            ),
            _ => (OwnedValue::Integer(0), CastTextToIntResultCode::NotInt),
        },
    }
}

#[derive(Debug, PartialEq)]
/// Reference
/// https://github.com/sqlite/sqlite/blob/master/src/util.c#L529
pub enum CastTextToRealResultCode {
    PureInt = 1,
    HasDecimal = 2,
    NotValid = 0,
    NotValidButPrefix = -1,
}

pub fn text_to_real(text: &str) -> (OwnedValue, CastTextToRealResultCode) {
    let text = text.trim();
    if text.is_empty() {
        return (OwnedValue::Float(0.0), CastTextToRealResultCode::NotValid);
    }
    let mut accum = String::new();
    let mut has_decimal_separator = false;
    let mut sign = false;
    let mut exp_sign = false;
    let mut has_exponent = false;
    let mut has_digit = false;
    let mut has_decimal_digit = false;
    let mut excess_space = false;

    let chars = text.chars();

    for c in chars {
        match c {
            '0'..='9' if !has_decimal_separator => {
                has_digit = true;
                accum.push(c);
            }
            '0'..='9' => {
                has_decimal_digit = true;
                accum.push(c);
            }
            '+' | '-' if !has_digit && !sign => {
                sign = true;
                accum.push(c);
            }
            '+' | '-' if has_exponent && !exp_sign => {
                exp_sign = true;
                accum.push(c);
            }
            '.' if !has_decimal_separator => {
                has_decimal_separator = true;
                accum.push(c);
            }
            'E' | 'e' if !has_decimal_separator || has_decimal_digit => {
                has_exponent = true;
                accum.push(c);
            }
            _ => {
                excess_space = true;
                break;
            }
        }
    }

    if let Ok(num) = accum.parse::<f64>() {
        if !has_decimal_separator && !exp_sign && !has_exponent {
            return (OwnedValue::Float(num), CastTextToRealResultCode::PureInt);
        }

        if excess_space {
            // TODO see if this branch satisfies: not a valid number, but has a valid prefix which
            // includes a decimal point and/or an eNNN clause
            return (
                OwnedValue::Float(num),
                CastTextToRealResultCode::NotValidButPrefix,
            );
        }

        return (OwnedValue::Float(num), CastTextToRealResultCode::HasDecimal);
    }

    return (OwnedValue::Float(0.0), CastTextToRealResultCode::NotValid);
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use limbo_sqlite3_parser::ast::{self, Expr, Id, Literal, Operator::*, Type};

    #[test]
    fn test_normalize_ident() {
        assert_eq!(normalize_ident("foo"), "foo");
        assert_eq!(normalize_ident("`foo`"), "foo");
        assert_eq!(normalize_ident("[foo]"), "foo");
        assert_eq!(normalize_ident("\"foo\""), "foo");
    }

    #[test]
    fn test_basic_addition_exprs_are_equivalent() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_addition_expressions_equivalent_normalized() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("243".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_subtraction_expressions_not_equivalent() {
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
        );
        assert!(!exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_subtraction_expressions_normalized() {
        let expr3 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22".to_string()))),
        );
        let expr4 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("66".to_string()))),
            Subtract,
            Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr3, &expr4));
    }

    #[test]
    fn test_expressions_equivalent_case_insensitive_functioncalls() {
        let func1 = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: None,
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        let func2 = Expr::FunctionCall {
            name: Id("sum".to_string()),
            distinctness: None,
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        assert!(exprs_are_equivalent(&func1, &func2));

        let func3 = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: Some(ast::Distinctness::Distinct),
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        assert!(!exprs_are_equivalent(&func1, &func3));
    }

    #[test]
    fn test_expressions_equivalent_identical_fn_with_distinct() {
        let sum = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: None,
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        let sum_distinct = Expr::FunctionCall {
            name: Id("SUM".to_string()),
            distinctness: Some(ast::Distinctness::Distinct),
            args: Some(vec![Expr::Id(Id("x".to_string()))]),
            order_by: None,
            filter_over: None,
        };
        assert!(!exprs_are_equivalent(&sum, &sum_distinct));
    }

    #[test]
    fn test_expressions_equivalent_multiplication() {
        let expr1 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("38".to_string()))),
        );
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
            Multiply,
            Box::new(Expr::Literal(Literal::Numeric("42".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_both_parenthesized_equivalent() {
        let expr1 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("799.0".to_string()))),
        )]);
        let expr2 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("799".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }
    #[test]
    fn test_expressions_parenthesized_equivalent() {
        let expr7 = Expr::Parenthesized(vec![Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
        )]);
        let expr8 = Expr::Binary(
            Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
            Add,
            Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
        );
        assert!(exprs_are_equivalent(&expr7, &expr8));
    }

    #[test]
    fn test_like_expressions_equivalent() {
        let expr1 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        let expr2 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        assert!(exprs_are_equivalent(&expr1, &expr2));
    }

    #[test]
    fn test_expressions_equivalent_like_escaped() {
        let expr1 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
        };
        let expr2 = Expr::Like {
            lhs: Box::new(Expr::Id(Id("name".to_string()))),
            not: false,
            op: ast::LikeOperator::Like,
            rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
            escape: Some(Box::new(Expr::Literal(Literal::String("#".to_string())))),
        };
        assert!(!exprs_are_equivalent(&expr1, &expr2));
    }
    #[test]
    fn test_expressions_equivalent_between() {
        let expr1 = Expr::Between {
            lhs: Box::new(Expr::Id(Id("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        let expr2 = Expr::Between {
            lhs: Box::new(Expr::Id(Id("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        assert!(exprs_are_equivalent(&expr1, &expr2));

        // differing BETWEEN bounds
        let expr3 = Expr::Between {
            lhs: Box::new(Expr::Id(Id("age".to_string()))),
            not: false,
            start: Box::new(Expr::Literal(Literal::Numeric("20".to_string()))),
            end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
        };
        assert!(!exprs_are_equivalent(&expr1, &expr3));
    }
    #[test]
    fn test_cast_exprs_equivalent() {
        let cast1 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
        };

        let cast2 = Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
            type_name: Some(Type {
                name: "integer".to_string(),
                size: None,
            }),
        };
        assert!(exprs_are_equivalent(&cast1, &cast2));
    }

    #[test]
    fn test_ident_equivalency() {
        assert!(check_ident_equivalency("\"foo\"", "foo"));
        assert!(check_ident_equivalency("[foo]", "foo"));
        assert!(check_ident_equivalency("`FOO`", "foo"));
        assert!(check_ident_equivalency("\"foo\"", "`FOO`"));
        assert!(!check_ident_equivalency("\"foo\"", "[bar]"));
        assert!(!check_ident_equivalency("foo", "\"bar\""));
    }

    #[test]
    fn test_text_to_integer() {
        let pairs = vec![
            (
                text_to_integer("1"),
                (OwnedValue::Integer(1), CastTextToIntResultCode::Success),
            ),
            (
                text_to_integer("-1"),
                (OwnedValue::Integer(-1), CastTextToIntResultCode::Success),
            ),
            (
                text_to_integer("10000000"),
                (
                    OwnedValue::Integer(10000000),
                    CastTextToIntResultCode::Success,
                ),
            ),
            (
                text_to_integer("-10000000"),
                (
                    OwnedValue::Integer(-10000000),
                    CastTextToIntResultCode::Success,
                ),
            ),
            (
                text_to_integer("xxx"),
                (OwnedValue::Integer(0), CastTextToIntResultCode::NotInt),
            ),
            (
                text_to_integer("123xxx"),
                (
                    OwnedValue::Integer(123),
                    CastTextToIntResultCode::ExcessSpace,
                ),
            ),
            (
                text_to_integer("9223372036854775807"),
                (
                    OwnedValue::Integer(i64::MAX),
                    CastTextToIntResultCode::Success,
                ),
            ),
            (
                text_to_integer("9223372036854775808"),
                (
                    OwnedValue::Integer(0),
                    CastTextToIntResultCode::TooLargeOrMalformed,
                ),
            ),
            (
                text_to_integer("-9223372036854775808"),
                (
                    OwnedValue::Integer(i64::MIN),
                    CastTextToIntResultCode::Success,
                ),
            ),
            (
                text_to_integer("-9223372036854775809"),
                (
                    OwnedValue::Integer(0),
                    CastTextToIntResultCode::TooLargeOrMalformed,
                ),
            ),
        ];

        for (left, right) in pairs {
            assert_eq!(left, right);
        }
    }

    #[test]
    fn test_text_to_real() {
        let pairs = vec![
            (
                text_to_real("1"),
                (OwnedValue::Float(1.0), CastTextToRealResultCode::PureInt),
            ),
            (
                text_to_real("-1"),
                (OwnedValue::Float(-1.0), CastTextToRealResultCode::PureInt),
            ),
            (
                text_to_real("1.0"),
                (OwnedValue::Float(1.0), CastTextToRealResultCode::HasDecimal),
            ),
            (
                text_to_real("-1.0"),
                (
                    OwnedValue::Float(-1.0),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("1e10"),
                (
                    OwnedValue::Float(1e10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("-1e10"),
                (
                    OwnedValue::Float(-1e10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("1e-10"),
                (
                    OwnedValue::Float(1e-10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("-1e-10"),
                (
                    OwnedValue::Float(-1e-10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("1.123e10"),
                (
                    OwnedValue::Float(1.123e10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("-1.123e10"),
                (
                    OwnedValue::Float(-1.123e10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("1.123e-10"),
                (
                    OwnedValue::Float(1.123e-10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("-1.123e-10"),
                (
                    OwnedValue::Float(-1.123e-10),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("1-282584294928"),
                (OwnedValue::Float(1.0), CastTextToRealResultCode::PureInt),
            ),
            (
                text_to_real("xxx"),
                (OwnedValue::Float(0.0), CastTextToRealResultCode::NotValid),
            ),
            (
                text_to_real("1.7976931348623157e308"),
                (
                    OwnedValue::Float(f64::MAX),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("1.7976931348623157e309"),
                (
                    OwnedValue::Float(f64::INFINITY),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("-1.7976931348623157e308"),
                (
                    OwnedValue::Float(f64::MIN),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
            (
                text_to_real("-1.7976931348623157e309"),
                (
                    OwnedValue::Float(f64::NEG_INFINITY),
                    CastTextToRealResultCode::HasDecimal,
                ),
            ),
        ];

        for (left, right) in pairs {
            assert_eq!(left, right);
        }
    }
}
