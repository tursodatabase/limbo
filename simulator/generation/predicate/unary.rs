//! Contains code for generation for [ast::Expr::Unary] Predicate

use limbo_sqlite3_parser::ast::{self, Expr};

use crate::{
    generation::{
        backtrack, pick, predicate::SimplePredicate, ArbitraryFrom as _, ArbitraryFromMaybe,
    },
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
            return Some(Self(Value::TRUE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
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
                        FalseValue::arbitrary_from_maybe(rng, &column_values).map(|value| {
                            Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
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
