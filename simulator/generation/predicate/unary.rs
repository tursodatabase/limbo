//! Contains code for generation for [ast::Expr::Unary] Predicate
//! TODO: for now just generating [ast::Literal], but want to also generate Columns and any
//! arbitrary [ast::Expr]

use limbo_sqlite3_parser::ast::{self, Expr};

use crate::{
    generation::{backtrack, pick, predicate::SimplePredicate, ArbitraryFromMaybe},
    model::{
        query::predicate::Predicate,
        table::{Table, Value},
    },
};

pub struct TrueValue(pub Value);

impl ArbitraryFromMaybe<&Value> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng>(_rng: &mut R, value: &Value) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a true value return it else you cannot return a true Value
        value.into_bool().then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&Value>> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng>(rng: &mut R, values: &Vec<&Value>) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(Value::TRUE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
    }
}

pub struct FalseValue(pub Value);

impl ArbitraryFromMaybe<&Value> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng>(_rng: &mut R, value: &Value) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a false value return it else you cannot return a false Value
        (!value.into_bool()).then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&Value>> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng>(rng: &mut R, values: &Vec<&Value>) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(Value::FALSE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
    }
}

pub struct BitNotValue(pub Value);

impl ArbitraryFromMaybe<(&Value, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng>(
        _rng: &mut R,
        (value, predicate): (&Value, bool),
    ) -> Option<Self>
    where
        Self: Sized,
    {
        let bit_not_val = value.unary_exec(ast::UnaryOperator::BitwiseNot);
        // If you bit not the Value and it meets the predicate return Some, else None
        (bit_not_val.into_bool() == predicate).then_some(BitNotValue(value.clone()))
    }
}

impl ArbitraryFromMaybe<(&Vec<&Value>, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng>(
        rng: &mut R,
        (values, predicate): (&Vec<&Value>, bool),
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
        SimplePredicate(Predicate(expr.unwrap_or(Expr::Literal(Value::TRUE.into()))))
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
        SimplePredicate(Predicate(expr.unwrap_or(Expr::Literal(Value::TRUE.into()))))
    }
}
