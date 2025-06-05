//! Contains code for generation for [ast::Expr::Binary] Predicate

use limbo_sqlite3_parser::ast::{self, Expr};

use crate::{
    generation::{
        backtrack, one_of,
        predicate::{CompoundPredicate, SimplePredicate},
        table::{GTValue, LTValue, LikeValue},
        ArbitraryFrom as _, ArbitraryFromMaybe as _,
    },
    model::{
        query::predicate::Predicate,
        table::{Table, Value},
    },
};

impl Predicate {
    /// Generate an [ast::Expr::Binary] [Predicate] from a column and [Value]
    pub fn from_column_binary<R: rand::Rng>(
        rng: &mut R,
        column_name: &str,
        value: &Value,
    ) -> Predicate {
        let expr = one_of(
            vec![
                Box::new(|_| {
                    Expr::Binary(
                        Box::new(Expr::Id(ast::Id(column_name.to_string()))),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, value).0;
                    Expr::Binary(
                        Box::new(Expr::Id(ast::Id(column_name.to_string()))),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, value).0;
                    Expr::Binary(
                        Box::new(Expr::Id(ast::Id(column_name.to_string()))),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
            ],
            rng,
        );
        Predicate(expr)
    }

    /// Produces a true [ast::Expr::Binary] [Predicate] that is true for the provided row in the given table
    pub fn true_binary<R: rand::Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
        // Pick a column
        let column_index = rng.gen_range(0..t.columns.len());
        let column = &t.columns[column_index];
        let value = &row[column_index];
        let predicate = backtrack(
            vec![
                (
                    1,
                    Box::new(|_| {
                        Some(Predicate(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name(t.name.clone()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Equals,
                            Box::new(Expr::Literal(value.into())),
                        )))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let v = Value::arbitrary_from(rng, &column.column_type);
                        if &v == value {
                            None
                        } else {
                            Some(Predicate(Expr::Binary(
                                Box::new(ast::Expr::Qualified(
                                    ast::Name(t.name.clone()),
                                    ast::Name(column.name.clone()),
                                )),
                                ast::Operator::NotEquals,
                                Box::new(Expr::Literal(v.into())),
                            )))
                        }
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let lt_value = LTValue::arbitrary_from(rng, value).0;
                        Some(Predicate(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name(t.name.clone()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Greater,
                            Box::new(Expr::Literal(lt_value.into())),
                        )))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let gt_value = GTValue::arbitrary_from(rng, value).0;
                        Some(Predicate(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name(t.name.clone()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Less,
                            Box::new(Expr::Literal(gt_value.into())),
                        )))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        LikeValue::arbitrary_from_maybe(rng, value).map(|like| {
                            Predicate(Expr::Like {
                                lhs: Box::new(ast::Expr::Qualified(
                                    ast::Name(t.name.clone()),
                                    ast::Name(column.name.clone()),
                                )),
                                not: false, // TODO: also generate this value eventually
                                op: ast::LikeOperator::Like,
                                rhs: Box::new(Expr::Literal(like.0.into())),
                                escape: None, // TODO: implement
                            })
                        })
                    }),
                ),
            ],
            rng,
        );
        predicate
    }

    /// Produces an [ast::Expr::Binary] [Predicate] that is false for the provided row in the given table
    pub fn false_binary<R: rand::Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
        // Pick a column
        let column_index = rng.gen_range(0..t.columns.len());
        let column = &t.columns[column_index];
        let value = &row[column_index];
        one_of(
            vec![
                Box::new(|_| {
                    Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::NotEquals,
                        Box::new(Expr::Literal(value.into())),
                    ))
                }),
                Box::new(|rng| {
                    let v = loop {
                        let v = Value::arbitrary_from(rng, &column.column_type);
                        if &v != value {
                            break v;
                        }
                    };
                    Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(v.into())),
                    ))
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, value).0;
                    Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    ))
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, value).0;
                    Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    ))
                }),
            ],
            rng,
        )
    }
}

impl SimplePredicate {
    /// Generates a true [ast::Expr::Binary] [SimplePredicate] from a [Table]
    pub fn true_binary<R: rand::Rng>(rng: &mut R, table: &Table, column_index: usize) -> Self {
        let column = &table.columns[column_index];
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        let expr = one_of(
            vec![
                Box::new(|rng| {
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Equals,
                        Box::new(Expr::arbitrary_from(rng, &column_values)),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, &column_values).0;
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, &column_values).0;
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
            ],
            rng,
        );
        SimplePredicate(Predicate(expr))
    }

    /// Generates a false [ast::Expr::Binary] [SimplePredicate] from a [Table]
    pub fn false_binary<R: rand::Rng>(rng: &mut R, table: &Table, column_index: usize) -> Self {
        let column = &table.columns[column_index];
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        let expr = one_of(
            vec![
                Box::new(|rng| {
                    Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::NotEquals,
                        Box::new(Expr::arbitrary_from(rng, &column_values)),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, &column_values).0;
                    Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, &column_values).0;
                    Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
            ],
            rng,
        );
        SimplePredicate(Predicate(expr))
    }
}

impl CompoundPredicate {
    /// Decide if you want to create an AND or an OR
    pub fn from_table_binary<R: rand::Rng>(
        rng: &mut R,
        table: &Table,
        predicate_value: bool,
    ) -> Self {
        let predicate = if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if predicate_value {
                (0..rng.gen_range(0..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, (table, true)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::And,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_()) // Empty And is True
            } else {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(0..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();

                let len = booleans.len();

                // Make sure at least one of them is false
                if !booleans.is_empty() && booleans.iter().all(|b| *b) {
                    booleans[rng.gen_range(0..len)] = false;
                }

                booleans
                    .iter()
                    .map(|b| SimplePredicate::arbitrary_from(rng, (table, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::And,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_()) // Empty And is True
            }
        } else {
            // An OR for true requires at least one of its children to be true
            // An OR for false requires each of its children to be false
            if predicate_value {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(0..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();
                let len = booleans.len();
                // Make sure at least one of them is true
                if !booleans.is_empty() && booleans.iter().all(|b| !*b) {
                    booleans[rng.gen_range(0..len)] = true;
                }

                booleans
                    .iter()
                    .map(|b| SimplePredicate::arbitrary_from(rng, (table, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::false_()) // Empty Or is False
            } else {
                (0..rng.gen_range(0..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, (table, false)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::false_()) // Empty Or is False
            }
        };
        Self(predicate)
    }
}
