use limbo_sqlite3_parser::ast::{self, Expr};
use rand::{seq::SliceRandom as _, Rng};

use crate::model::{
    query::predicate::Predicate,
    table::{SimValue, Table},
};

use super::{one_of, ArbitraryFrom};

mod binary;
mod unary;

#[derive(Debug)]
struct CompoundPredicate(Predicate);

#[derive(Debug)]
struct SimplePredicate(Predicate);

impl ArbitraryFrom<(&Table, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        let choice = rng.gen_range(0..2);
        // Pick an operator
        match predicate_value {
            true => match choice {
                0 => SimplePredicate::true_binary(rng, table),
                1 => SimplePredicate::true_unary(rng, table),
                _ => unreachable!(),
            },
            false => match choice {
                0 => SimplePredicate::false_binary(rng, table),
                1 => SimplePredicate::false_unary(rng, table),
                _ => unreachable!(),
            },
        }
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        CompoundPredicate::from_table_binary(rng, table, predicate_value)
    }
}

impl ArbitraryFrom<&Table> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let predicate_value = rng.gen_bool(0.5);
        CompoundPredicate::arbitrary_from(rng, (table, predicate_value)).0
    }
}

impl ArbitraryFrom<(&str, &SimValue)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (column_name, value): (&str, &SimValue)) -> Self {
        Predicate::from_column_binary(rng, column_name, value)
    }
}

impl ArbitraryFrom<(&Table, &Vec<SimValue>)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<SimValue>)) -> Self {
        // We want to produce a predicate that is true for the row
        // We can do this by creating several predicates that
        // are true, some that are false, combiend them in ways that correspond to the creation of a true predicate

        // Produce some true and false predicates
        let mut true_predicates = (1..=rng.gen_range(1..=4))
            .map(|_| Predicate::true_binary(rng, t, row))
            .collect::<Vec<_>>();

        let false_predicates = (0..=rng.gen_range(0..=3))
            .map(|_| Predicate::false_binary(rng, t, row))
            .collect::<Vec<_>>();

        // Start building a top level predicate from a true predicate
        let mut result = true_predicates.pop().unwrap();

        let mut predicates = true_predicates
            .iter()
            .map(|p| (true, p.clone()))
            .chain(false_predicates.iter().map(|p| (false, p.clone())))
            .collect::<Vec<_>>();

        predicates.shuffle(rng);

        while !predicates.is_empty() {
            // Create a new predicate from at least 1 and at most 3 predicates
            let context =
                predicates[0..rng.gen_range(0..=usize::min(3, predicates.len()))].to_vec();
            // Shift `predicates` to remove the predicates in the context
            predicates = predicates[context.len()..].to_vec();

            // `result` is true, so we have the following three options to make a true predicate:
            // T or F
            // T or T
            // T and T

            result = one_of(
                vec![
                    // T or (X1 or X2 or ... or Xn)
                    Box::new(|_| {
                        Predicate(Expr::Binary(
                            Box::new(result.0.clone()),
                            ast::Operator::Or,
                            Box::new(
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .reduce(|accum, curr| {
                                        Predicate(Expr::Binary(
                                            Box::new(accum.0),
                                            ast::Operator::Or,
                                            Box::new(curr.0),
                                        ))
                                    })
                                    .unwrap_or(Predicate::false_())
                                    .0,
                            ),
                        ))
                    }),
                    // T or (T1 and T2 and ... and Tn)
                    Box::new(|_| {
                        Predicate(Expr::Binary(
                            Box::new(result.0.clone()),
                            ast::Operator::Or,
                            Box::new(
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .reduce(|accum, curr| {
                                        Predicate(Expr::Binary(
                                            Box::new(accum.0),
                                            ast::Operator::And,
                                            Box::new(curr.0),
                                        ))
                                    })
                                    .unwrap_or(Predicate::true_())
                                    .0,
                            ),
                        ))
                    }),
                    // T and T
                    Box::new(|_| {
                        // Check if all the predicates in the context are true
                        if context.iter().all(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate(Expr::Binary(
                                Box::new(result.0.clone()),
                                ast::Operator::And,
                                Box::new(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .reduce(|accum, curr| {
                                            Predicate(Expr::Binary(
                                                Box::new(accum.0),
                                                ast::Operator::And,
                                                Box::new(curr.0),
                                            ))
                                        })
                                        .unwrap_or(Predicate::true_())
                                        .0,
                                ),
                            ))
                        }
                        // Check if there is at least one true predicate
                        else if context.iter().any(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate(Expr::Binary(
                                Box::new(result.0.clone()),
                                ast::Operator::And,
                                Box::new(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .reduce(|accum, curr| {
                                            Predicate(Expr::Binary(
                                                Box::new(accum.0),
                                                ast::Operator::Or,
                                                Box::new(curr.0),
                                            ))
                                        })
                                        .unwrap_or(Predicate::false_())
                                        .0,
                                ),
                            ))
                            // Predicate::And(vec![
                            //     result.clone(),
                            //     Predicate::Or(context.iter().map(|(_, p)| p.clone()).collect()),
                            // ])
                        } else {
                            // T and (X1 or X2 or ... or Xn or TRUE)
                            Predicate(Expr::Binary(
                                Box::new(result.0.clone()),
                                ast::Operator::And,
                                Box::new(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .chain(std::iter::once(Predicate::true_()))
                                        .reduce(|accum, curr| {
                                            Predicate(Expr::Binary(
                                                Box::new(accum.0),
                                                ast::Operator::Or,
                                                Box::new(curr.0),
                                            ))
                                        })
                                        .unwrap() // Chain guarantees at least one value
                                        .0,
                                ),
                            ))
                        }
                    }),
                ],
                rng,
            );
        }
        result
    }
}
