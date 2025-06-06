//! Contains code for generation for [ast::Expr::Unary] Predicate
//! TODO: for now just generating [ast::Literal], but want to also generate Columns and any
//! arbitrary [ast::Expr]

use limbo_sqlite3_parser::ast::{self, Expr};

use crate::{
    generation::{backtrack, pick, predicate::SimplePredicate, ArbitraryFromMaybe},
    model::{
        query::predicate::Predicate,
        table::{SimValue, Table},
    },
};

pub struct TrueValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng>(_rng: &mut R, value: &SimValue) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a true value return it else you cannot return a true Value
        value.into_bool().then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&SimValue>> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng>(rng: &mut R, values: &Vec<&SimValue>) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(SimValue::TRUE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
    }
}

pub struct FalseValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng>(_rng: &mut R, value: &SimValue) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a false value return it else you cannot return a false Value
        (!value.into_bool()).then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&SimValue>> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng>(rng: &mut R, values: &Vec<&SimValue>) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(SimValue::FALSE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
    }
}

pub struct BitNotValue(pub SimValue);

impl ArbitraryFromMaybe<(&SimValue, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng>(
        _rng: &mut R,
        (value, predicate): (&SimValue, bool),
    ) -> Option<Self>
    where
        Self: Sized,
    {
        let bit_not_val = value.unary_exec(ast::UnaryOperator::BitwiseNot);
        // If you bit not the Value and it meets the predicate return Some, else None
        (bit_not_val.into_bool() == predicate).then_some(BitNotValue(value.clone()))
    }
}

impl ArbitraryFromMaybe<(&Vec<&SimValue>, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng>(
        rng: &mut R,
        (values, predicate): (&Vec<&SimValue>, bool),
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return None;
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, (*value, predicate))
    }
}

// TODO: have some more complex generation with columns names here as well
impl SimplePredicate {
    /// Generates a true [ast::Expr::Unary] [SimplePredicate] from a [Table]
    pub fn true_unary<R: rand::Rng>(rng: &mut R, table: &Table, column_index: usize) -> Self {
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        let num_retries = column_values.len();
        let expr = backtrack(
            vec![
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            // Positive is a no-op in Sqlite
                            Expr::unary(ast::UnaryOperator::Positive, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            // True Value with negative is still True
                            Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        BitNotValue::arbitrary_from_maybe(rng, (&column_values, true)).map(
                            |value| {
                                Expr::unary(
                                    ast::UnaryOperator::BitwiseNot,
                                    Expr::Literal(value.0.into()),
                                )
                            },
                        )
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            Expr::unary(ast::UnaryOperator::Not, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
            ],
            rng,
        );
        // If cannot generate a value
        SimplePredicate(Predicate(
            expr.unwrap_or(Expr::Literal(SimValue::TRUE.into())),
        ))
    }

    /// Generates a false [ast::Expr::Unary] [SimplePredicate] from a [Table]
    pub fn false_unary<R: rand::Rng>(rng: &mut R, table: &Table, column_index: usize) -> Self {
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        let num_retries = column_values.len();
        let expr = backtrack(
            vec![
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            // Positive is a no-op in Sqlite
                            Expr::unary(ast::UnaryOperator::Positive, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            // True Value with negative is still True
                            Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        BitNotValue::arbitrary_from_maybe(rng, (&column_values, false)).map(
                            |value| {
                                Expr::unary(
                                    ast::UnaryOperator::BitwiseNot,
                                    Expr::Literal(value.0.into()),
                                )
                            },
                        )
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            Expr::unary(ast::UnaryOperator::Not, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
            ],
            rng,
        );
        // If cannot generate a value
        SimplePredicate(Predicate(
            expr.unwrap_or(Expr::Literal(SimValue::TRUE.into())),
        ))
    }
}
