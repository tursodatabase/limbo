use std::{collections::HashSet, fs::File};

use anarchist_readable_name_generator_lib::readable_name_custom;
use antithesis_sdk::random::{get_random, AntithesisRng};
use limbo_sim::{
    generation::{pick_index, Arbitrary, ArbitraryFrom},
    model::{
        ast,
        query::{
            create::Create, delete::Delete, insert::Insert, predicate::Predicate, update::Update,
        },
        table::{SimValue, Table},
        SimulatorEnv,
    },
};
use std::io::Write;

use crate::{opts::Opts, Plan};

/// Represents a complete SQLite schema
#[derive(Debug, Clone)]
pub struct ArbitrarySchema {
    pub tables: Vec<Table>,
}

impl SimulatorEnv for ArbitrarySchema {
    fn tables(&self) -> &[limbo_sim::model::table::Table] {
        &self.tables
    }

    fn tables_mut(&mut self) -> &mut [limbo_sim::model::table::Table] {
        &mut self.tables
    }

    fn add_table(&mut self, table: limbo_sim::model::table::Table) {
        self.tables.push(table);
    }

    fn remove_table(&mut self, table_name: &str) {
        self.tables.retain(|t| t.name != table_name);
    }

    fn opts(&self) -> &limbo_sim::model::SimulatorOpts {
        todo!("only used for property or plan generation")
    }

    fn connections(&self) -> &[limbo_sim::model::SimConnection] {
        todo!("only used for property or plan generation")
    }

    fn connections_mut(&mut self) -> &mut [limbo_sim::model::SimConnection] {
        todo!("only used for property or plan generation")
    }
}

// Helper functions for generating random data
fn generate_random_identifier() -> String {
    readable_name_custom("_", AntithesisRng).replace('-', "_")
}

fn generate_random_table() -> Table {
    let mut rng = AntithesisRng;
    let mut table = Table::arbitrary(&mut rng);
    let pk_index = pick_index(table.columns.len(), &mut rng);
    table.columns[pk_index].primary = true;

    table
}

pub fn gen_schema() -> ArbitrarySchema {
    let table_count = (get_random() % 10 + 1) as usize;
    let mut tables = Vec::with_capacity(table_count);
    let mut table_names = HashSet::new();

    for _ in 0..table_count {
        let mut table = generate_random_table();

        // Ensure table names are unique
        while table_names.contains(&table.name) {
            table.name = generate_random_identifier();
        }

        table_names.insert(table.name.clone());
        tables.push(table);
    }

    ArbitrarySchema { tables }
}

impl ArbitrarySchema {
    /// Convert the schema to a vector of SQL DDL statements
    pub fn to_sql(&self) -> Vec<String> {
        // TODO: inneficient to clone the tables to just print it them
        // But this is a limitation of the struct creation process
        self.tables
            .clone()
            .into_iter()
            .map(|table| Create { table }.to_string())
            .collect()
    }

    /// Generate a random SQL statement for a schema
    ///
    /// TODO: as we do not yet update the rows inside the tables in [ArbitrarySchema]
    /// we cannot generate many more complex predicates in the where clause
    fn generate_random_statement(&self) -> String {
        match get_random() % 3 {
            0 => Insert::arbitrary_from(&mut AntithesisRng, self).to_string(),
            1 => {
                let mut update = Update::arbitrary_from(&mut AntithesisRng, self);
                // TODO: to mimic the previous generation strategy I am adjusting the where clause here
                // to filter by the primary key
                // Ideally, we would want arbitrary_from to generate the random predicate for us

                let table = self.tables.iter().find(|t| t.name == update.table).unwrap();
                // Currently for stress testing, we have a single column with Primary Key
                let pk_col = table
                    .columns
                    .iter()
                    .find(|col| col.primary)
                    .expect("Table should have a primary key");
                update.predicate = Predicate(ast::Expr::Binary(
                    Box::new(ast::Expr::Qualified(
                        ast::Name(table.name.clone()),
                        ast::Name(pk_col.name.clone()),
                    )),
                    ast::Operator::Equals,
                    Box::new(ast::Expr::Literal(
                        SimValue::arbitrary_from(&mut AntithesisRng, &pk_col.column_type).into(),
                    )),
                ));
                update.to_string()
            }
            _ => {
                let mut delete = Delete::arbitrary_from(&mut AntithesisRng, self);

                // TODO: to mimic the previous generation strategy I am adjusting the where clause here
                // to filter by the primary key
                // Ideally, we would want arbitrary_from to generate the random predicate for us

                let table = self.tables.iter().find(|t| t.name == delete.table).unwrap();
                // Currently for stress testing, we have a single column with Primary Key
                let pk_col = table
                    .columns
                    .iter()
                    .find(|col| col.primary)
                    .expect("Table should have a primary key");
                delete.predicate = Predicate(ast::Expr::Binary(
                    Box::new(ast::Expr::Qualified(
                        ast::Name(table.name.clone()),
                        ast::Name(pk_col.name.clone()),
                    )),
                    ast::Operator::Equals,
                    Box::new(ast::Expr::Literal(
                        SimValue::arbitrary_from(&mut AntithesisRng, &pk_col.column_type).into(),
                    )),
                ));

                delete.to_string()
            }
        }
    }
}

pub fn generate_plan(opts: &Opts) -> Result<Plan, Box<dyn std::error::Error + Send + Sync>> {
    let schema = gen_schema();
    // Write DDL statements to log file
    let mut log_file = File::create(&opts.log_file)?;
    let ddl_statements = schema.to_sql();
    let mut plan = Plan {
        ddl_statements: vec![],
        queries_per_thread: vec![],
        nr_iterations: opts.nr_iterations,
        nr_threads: opts.nr_threads,
    };
    if !opts.skip_log {
        writeln!(log_file, "{}", opts.nr_threads)?;
        writeln!(log_file, "{}", opts.nr_iterations)?;
        writeln!(log_file, "{}", ddl_statements.len())?;
        for stmt in &ddl_statements {
            writeln!(log_file, "{}", stmt)?;
        }
    }
    plan.ddl_statements = ddl_statements;
    for _ in 0..opts.nr_threads {
        let mut queries = vec![];
        for _ in 0..opts.nr_iterations {
            let sql = schema.generate_random_statement();
            if !opts.skip_log {
                writeln!(log_file, "{}", sql)?;
            }
            queries.push(sql);
        }
        plan.queries_per_thread.push(queries);
    }
    Ok(plan)
}
