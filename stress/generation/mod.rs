use std::{collections::HashSet, fs::File};

use anarchist_readable_name_generator_lib::readable_name_custom;
use antithesis_sdk::random::{get_random, AntithesisRng};
use limbo_sim::{
    generation::{pick_index, Arbitrary, ArbitraryFrom},
    model::{
        query::{create::Create, delete::Delete, insert::Insert, update::Update},
        table::Table,
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
    fn generate_random_statement(&self) -> String {
        match get_random() % 3 {
            0 => Insert::arbitrary_from(&mut AntithesisRng, self).to_string(),
            1 => Update::arbitrary_from(&mut AntithesisRng, self).to_string(),
            _ => Delete::arbitrary_from(&mut AntithesisRng, self).to_string(),
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
