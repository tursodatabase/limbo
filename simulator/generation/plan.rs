use std::{fmt::Display, path::Path, rc::Rc, vec};

use limbo_core::{Connection, Result, StepResult};
use serde::{Deserialize, Serialize};

use crate::{
    model::{
        query::{
            select::{Distinctness, Predicate, ResultColumn},
            Create, Delete, Drop, Insert, Query, Select,
        },
        table::Value,
    },
    runner::env::SimConnection,
    SimulatorEnv,
};

use crate::generation::{frequency, Arbitrary, ArbitraryFrom};

use super::property::{remaining, Property};

pub(crate) type ResultSet = Result<Vec<Vec<Value>>>;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct InteractionPlan {
    pub(crate) plan: Vec<Interactions>,
}

impl InteractionPlan {
    /// Compute via diff computes a a plan from a given `.plan` file without the need to parse
    /// sql. This is possible because there are two versions of the plan file, one that is human
    /// readable and one that is serialized as JSON. Under watch mode, the users will be able to
    /// delete interactions from the human readable file, and this function uses the JSON file as
    /// a baseline to detect with interactions were deleted and constructs the plan from the
    /// remaining interactions.
    pub(crate) fn compute_via_diff(plan_path: &Path) -> Vec<Vec<Interaction>> {
        let interactions = std::fs::read_to_string(plan_path).unwrap();
        let interactions = interactions.lines().collect::<Vec<_>>();

        let plan: InteractionPlan = serde_json::from_str(
            std::fs::read_to_string(plan_path.with_extension("json"))
                .unwrap()
                .as_str(),
        )
        .unwrap();

        let mut plan = plan
            .plan
            .into_iter()
            .map(|i| i.interactions())
            .collect::<Vec<_>>();

        let (mut i, mut j) = (0, 0);

        while i < interactions.len() && j < plan.len() {
            if interactions[i].starts_with("-- begin")
                || interactions[i].starts_with("-- end")
                || interactions[i].is_empty()
            {
                i += 1;
                continue;
            }

            // interactions[i] is the i'th line in the human readable plan
            // plan[j][k] is the k'th interaction in the j'th property
            let mut k = 0;

            while k < plan[j].len() {
                if i >= interactions.len() {
                    let _ = plan.split_off(j + 1);
                    let _ = plan[j].split_off(k);
                    break;
                }
                if interactions[i].contains(plan[j][k].to_string().as_str()) {
                    i += 1;
                    k += 1;
                } else {
                    plan[j].remove(k);
                }
            }

            if plan[j].is_empty() {
                plan.remove(j);
            } else {
                j += 1;
            }
        }
        let _ = plan.split_off(j);
        plan
    }
}

pub(crate) struct InteractionPlanState {
    pub(crate) stack: Vec<ResultSet>,
    pub(crate) interaction_pointer: usize,
    pub(crate) secondary_pointer: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Interactions {
    Property(Property),
    Query(Query),
    Fault(Fault),
}

impl Interactions {
    pub(crate) fn name(&self) -> Option<String> {
        match self {
            Interactions::Property(property) => Some(property.name()),
            Interactions::Query(_) => None,
            Interactions::Fault(_) => None,
        }
    }

    pub(crate) fn interactions(&self) -> Vec<Interaction> {
        match self {
            Interactions::Property(property) => property.interactions(),
            Interactions::Query(query) => vec![Interaction::Query(query.clone())],
            Interactions::Fault(fault) => vec![Interaction::Fault(fault.clone())],
        }
    }
}

impl Interactions {
    pub(crate) fn dependencies(&self) -> Vec<String> {
        match self {
            Interactions::Property(property) => {
                property
                    .interactions()
                    .iter()
                    .fold(vec![], |mut acc, i| match i {
                        Interaction::Query(q) => {
                            acc.extend(q.dependencies());
                            acc
                        }
                        _ => acc,
                    })
            }
            Interactions::Query(query) => query.dependencies(),
            Interactions::Fault(_) => vec![],
        }
    }

    pub(crate) fn uses(&self) -> Vec<String> {
        match self {
            Interactions::Property(property) => {
                property
                    .interactions()
                    .iter()
                    .fold(vec![], |mut acc, i| match i {
                        Interaction::Query(q) => {
                            acc.extend(q.uses());
                            acc
                        }
                        _ => acc,
                    })
            }
            Interactions::Query(query) => query.uses(),
            Interactions::Fault(_) => vec![],
        }
    }
}

impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for interactions in &self.plan {
            match interactions {
                Interactions::Property(property) => {
                    let name = property.name();
                    writeln!(f, "-- begin testing '{}'", name)?;
                    for interaction in property.interactions() {
                        write!(f, "\t")?;

                        match interaction {
                            Interaction::Query(query) => writeln!(f, "{};", query)?,
                            Interaction::Assumption(assumption) => {
                                writeln!(f, "-- ASSUME {};", assumption.message)?
                            }
                            Interaction::Assertion(assertion) => {
                                writeln!(f, "-- ASSERT {};", assertion.message)?
                            }
                            Interaction::Fault(fault) => writeln!(f, "-- FAULT '{}';", fault)?,
                        }
                    }
                    writeln!(f, "-- end testing '{}'", name)?;
                }
                Interactions::Fault(fault) => {
                    writeln!(f, "-- FAULT '{}'", fault)?;
                }
                Interactions::Query(query) => {
                    writeln!(f, "{};", query)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InteractionStats {
    pub(crate) read_count: usize,
    pub(crate) write_count: usize,
    pub(crate) delete_count: usize,
    pub(crate) create_count: usize,
    pub(crate) drop_count: usize,
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Write: {}, Delete: {}, Create: {}, Drop: {}",
            self.read_count,
            self.write_count,
            self.delete_count,
            self.create_count,
            self.drop_count
        )
    }
}

pub(crate) enum Interaction {
    Query(Query),
    Assumption(Assertion),
    Assertion(Assertion),
    Fault(Fault),
}

impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{}", query),
            Self::Assumption(assumption) => write!(f, "ASSUME {}", assumption.message),
            Self::Assertion(assertion) => write!(f, "ASSERT {}", assertion.message),
            Self::Fault(fault) => write!(f, "FAULT '{}'", fault),
        }
    }
}

type AssertionFunc = dyn Fn(&Vec<ResultSet>, &SimulatorEnv) -> Result<bool>;

enum AssertionAST {
    Pick(),
}

pub(crate) struct Assertion {
    pub(crate) func: Box<AssertionFunc>,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Fault {
    Disconnect,
}

impl Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fault::Disconnect => write!(f, "DISCONNECT"),
        }
    }
}

impl Interactions {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
        match self {
            Interactions::Property(property) => {
                match property {
                    Property::InsertValuesSelect {
                        insert,
                        row_index: _,
                        queries,
                        select,
                    } => {
                        insert.shadow(env);
                        for query in queries {
                            query.shadow(env);
                        }
                        select.shadow(env);
                    }
                    Property::DoubleCreateFailure { create, queries } => {
                        if env.tables.iter().any(|t| t.name == create.table.name) {
                            return;
                        }
                        create.shadow(env);
                        for query in queries {
                            query.shadow(env);
                        }
                    }
                    Property::SelectLimit { select } => {
                        select.shadow(env);
                    }
                    Property::DeleteSelect {
                        table,
                        predicate,
                        queries,
                    } => {
                        let delete = Query::Delete(Delete {
                            table: table.clone(),
                            predicate: predicate.clone(),
                        });

                        let select = Query::Select(Select {
                            table: table.clone(),
                            result_columns: vec![ResultColumn::Star],
                            predicate: predicate.clone(),
                            distinct: Distinctness::All,
                            limit: None,
                        });

                        delete.shadow(env);
                        for query in queries {
                            query.shadow(env);
                        }
                        select.shadow(env);
                    }
                    Property::DropSelect {
                        table,
                        queries,
                        select,
                    } => {
                        let drop = Query::Drop(Drop {
                            table: table.clone(),
                        });

                        drop.shadow(env);
                        for query in queries {
                            query.shadow(env);
                        }
                        select.shadow(env);
                    }
                    Property::SelectSelectOptimizer { table, predicate } => {
                        let select1 = Query::Select(Select {
                            table: table.clone(),
                            result_columns: vec![ResultColumn::Expr(predicate.clone())],
                            predicate: Predicate::true_(),
                            distinct: Distinctness::All,
                            limit: None,
                        });

                        let select2 = Query::Select(Select {
                            table: table.clone(),
                            result_columns: vec![ResultColumn::Star],
                            predicate: predicate.clone(),
                            distinct: Distinctness::All,
                            limit: None,
                        });

                        select1.shadow(env);
                        select2.shadow(env);
                    }
                }
                for interaction in property.interactions() {
                    match interaction {
                        Interaction::Query(query) => match query {
                            Query::Create(create) => {
                                create.shadow(env);
                            }
                            Query::Insert(insert) => {
                                insert.shadow(env);
                            }
                            Query::Delete(delete) => {
                                delete.shadow(env);
                            }
                            Query::Drop(drop) => {
                                drop.shadow(env);
                            }
                            Query::Select(select) => {
                                select.shadow(env);
                            }
                        },
                        Interaction::Assertion(_) => {}
                        Interaction::Assumption(_) => {}
                        Interaction::Fault(_) => {}
                    }
                }
            }
            Interactions::Query(query) => {
                query.shadow(env);
            }
            Interactions::Fault(_) => {}
        }
    }
}

impl InteractionPlan {
    pub(crate) fn new() -> Self {
        Self { plan: Vec::new() }
    }

    pub(crate) fn stats(&self) -> InteractionStats {
        let mut read = 0;
        let mut write = 0;
        let mut delete = 0;
        let mut create = 0;
        let mut drop = 0;

        for interactions in &self.plan {
            match interactions {
                Interactions::Property(property) => {
                    for interaction in &property.interactions() {
                        if let Interaction::Query(query) = interaction {
                            match query {
                                Query::Select(_) => read += 1,
                                Query::Insert(_) => write += 1,
                                Query::Delete(_) => delete += 1,
                                Query::Create(_) => create += 1,
                                Query::Drop(_) => drop += 1,
                            }
                        }
                    }
                }
                Interactions::Query(query) => match query {
                    Query::Select(_) => read += 1,
                    Query::Insert(_) => write += 1,
                    Query::Delete(_) => delete += 1,
                    Query::Create(_) => create += 1,
                    Query::Drop(_) => drop += 1,
                },
                Interactions::Fault(_) => {}
            }
        }

        InteractionStats {
            read_count: read,
            write_count: write,
            delete_count: delete,
            create_count: create,
            drop_count: drop,
        }
    }
}

impl ArbitraryFrom<&mut SimulatorEnv> for InteractionPlan {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new();

        let num_interactions = env.opts.max_interactions;

        // First create at least one table
        let create_query = Create::arbitrary(rng);
        env.tables.push(create_query.table.clone());

        plan.plan
            .push(Interactions::Query(Query::Create(create_query)));

        while plan.plan.len() < num_interactions {
            log::debug!(
                "Generating interaction {}/{}",
                plan.plan.len(),
                num_interactions
            );
            let interactions = Interactions::arbitrary_from(rng, (env, plan.stats()));
            interactions.shadow(env);

            plan.plan.push(interactions);
        }

        log::info!("Generated plan with {} interactions", plan.plan.len());
        plan
    }
}

impl Interaction {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        match self {
            Self::Query(query) => query.shadow(env),
            Self::Assumption(_) | Self::Assertion(_) | Self::Fault(_) => vec![],
        }
    }
    pub(crate) fn execute_query(&self, conn: &mut Rc<Connection>) -> ResultSet {
        if let Self::Query(query) = self {
            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                log::debug!(
                    "Error running query '{}': {:?}",
                    &query_str[0..query_str.len().min(4096)],
                    err
                );
                return Err(err.unwrap());
            }
            let rows = rows?;
            assert!(rows.is_some());
            let mut rows = rows.unwrap();
            let mut out = Vec::new();
            while let Ok(row) = rows.step() {
                match row {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = match v {
                                limbo_core::Value::Null => Value::Null,
                                limbo_core::Value::Integer(i) => Value::Integer(*i),
                                limbo_core::Value::Float(f) => Value::Float(*f),
                                limbo_core::Value::Text(t) => Value::Text(t.as_str().to_string()),
                                limbo_core::Value::Blob(b) => Value::Blob(b.to_vec()),
                            };
                            r.push(v);
                        }
                        out.push(r);
                    }
                    StepResult::IO => {}
                    StepResult::Interrupt => {}
                    StepResult::Done => {
                        break;
                    }
                    StepResult::Busy => {}
                }
            }

            Ok(out)
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }

    pub(crate) fn execute_assertion(
        &self,
        stack: &Vec<ResultSet>,
        env: &SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Query(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
            Self::Assertion(assertion) => {
                let result = assertion.func.as_ref()(stack, env);
                match result {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(limbo_core::LimboError::InternalError(
                        assertion.message.clone(),
                    )),
                    Err(err) => Err(limbo_core::LimboError::InternalError(format!(
                        "{}. Inner error: {}",
                        assertion.message, err
                    ))),
                }
            }
            Self::Assumption(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
            Self::Fault(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
        }
    }

    pub(crate) fn execute_assumption(
        &self,
        stack: &Vec<ResultSet>,
        env: &SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Query(_) => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
            Self::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
            Self::Assumption(assumption) => {
                let result = assumption.func.as_ref()(stack, env);
                match result {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(limbo_core::LimboError::InternalError(
                        assumption.message.clone(),
                    )),
                    Err(err) => Err(limbo_core::LimboError::InternalError(format!(
                        "{}. Inner error: {}",
                        assumption.message, err
                    ))),
                }
            }
            Self::Fault(_) => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
        }
    }

    pub(crate) fn execute_fault(&self, env: &mut SimulatorEnv, conn_index: usize) -> Result<()> {
        match self {
            Self::Query(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Self::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Self::Assumption(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Self::Fault(fault) => {
                match fault {
                    Fault::Disconnect => {
                        if env.connections[conn_index].is_connected() {
                            env.connections[conn_index].disconnect();
                        } else {
                            return Err(limbo_core::LimboError::InternalError(
                                "connection already disconnected".into(),
                            ));
                        }
                        env.connections[conn_index] = SimConnection::Disconnected;
                    }
                }
                Ok(())
            }
        }
    }
}

fn random_create<R: rand::Rng>(rng: &mut R, _env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Create(Create::arbitrary(rng)))
}

fn random_read<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Select(Select::arbitrary_from(rng, env)))
}

fn random_write<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Insert(Insert::arbitrary_from(rng, env)))
}

fn random_delete<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Delete(Delete::arbitrary_from(rng, env)))
}

fn random_drop<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Drop(Drop::arbitrary_from(rng, env)))
}

fn random_fault<R: rand::Rng>(_rng: &mut R, _env: &SimulatorEnv) -> Interactions {
    Interactions::Fault(Fault::Disconnect)
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats)> for Interactions {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): (&SimulatorEnv, InteractionStats),
    ) -> Self {
        let remaining_ = remaining(env, &stats);
        frequency(
            vec![
                (
                    f64::min(remaining_.read, remaining_.write) + remaining_.create,
                    Box::new(|rng: &mut R| {
                        Interactions::Property(Property::arbitrary_from(rng, (env, &stats)))
                    }),
                ),
                (
                    remaining_.read,
                    Box::new(|rng: &mut R| random_read(rng, env)),
                ),
                (
                    remaining_.write,
                    Box::new(|rng: &mut R| random_write(rng, env)),
                ),
                (
                    remaining_.create,
                    Box::new(|rng: &mut R| random_create(rng, env)),
                ),
                (
                    remaining_.delete,
                    Box::new(|rng: &mut R| random_delete(rng, env)),
                ),
                (
                    // remaining_.drop,
                    0.0,
                    Box::new(|rng: &mut R| random_drop(rng, env)),
                ),
                (
                    remaining_
                        .read
                        .min(remaining_.write)
                        .min(remaining_.create)
                        .max(1.0),
                    Box::new(|rng: &mut R| random_fault(rng, env)),
                ),
            ],
            rng,
        )
    }
}
