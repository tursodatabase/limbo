use std::{collections::HashSet, fs::File};

use anarchist_readable_name_generator_lib::readable_name_custom;
use antithesis_sdk::random::{get_random, AntithesisRng};
use std::io::Write;

use crate::{opts::Opts, Plan};

/// Represents a column in a SQLite table
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

/// Represents SQLite data types
#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

/// Represents column constraints
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
}

/// Represents a table in a SQLite schema
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Represents a complete SQLite schema
#[derive(Debug, Clone)]
pub struct ArbitrarySchema {
    pub tables: Vec<Table>,
}

// Helper functions for generating random data
fn generate_random_identifier() -> String {
    readable_name_custom("_", AntithesisRng).replace('-', "_")
}

fn generate_random_data_type() -> DataType {
    match get_random() % 5 {
        0 => DataType::Integer,
        1 => DataType::Real,
        2 => DataType::Text,
        3 => DataType::Blob,
        _ => DataType::Numeric,
    }
}

fn generate_random_constraint() -> Constraint {
    match get_random() % 2 {
        0 => Constraint::NotNull,
        _ => Constraint::Unique,
    }
}

fn generate_random_column() -> Column {
    let name = generate_random_identifier();
    let data_type = generate_random_data_type();

    let constraint_count = (get_random() % 3) as usize;
    let mut constraints = Vec::with_capacity(constraint_count);

    for _ in 0..constraint_count {
        constraints.push(generate_random_constraint());
    }

    Column {
        name,
        data_type,
        constraints,
    }
}

fn generate_random_table() -> Table {
    let name = generate_random_identifier();
    let column_count = (get_random() % 10 + 1) as usize;
    let mut columns = Vec::with_capacity(column_count);
    let mut column_names = HashSet::new();

    // First, generate all columns without primary keys
    for _ in 0..column_count {
        let mut column = generate_random_column();

        // Ensure column names are unique within the table
        while column_names.contains(&column.name) {
            column.name = generate_random_identifier();
        }

        column_names.insert(column.name.clone());
        columns.push(column);
    }

    // Then, randomly select one column to be the primary key
    let pk_index = (get_random() % column_count as u64) as usize;
    columns[pk_index].constraints.push(Constraint::PrimaryKey);

    Table { name, columns }
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
        self.tables
            .iter()
            .map(|table| {
                let columns = table
                    .columns
                    .iter()
                    .map(|col| {
                        let mut col_def =
                            format!("  {} {}", col.name, data_type_to_sql(&col.data_type));
                        if cfg!(feature = "index_experimental") {
                            for constraint in &col.constraints {
                                col_def.push(' ');
                                col_def.push_str(&constraint_to_sql(constraint));
                            }
                        }
                        col_def
                    })
                    .collect::<Vec<_>>()
                    .join(",");

                format!("CREATE TABLE {} ({});", table.name, columns)
            })
            .collect()
    }
}

fn data_type_to_sql(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Integer => "INTEGER",
        DataType::Real => "REAL",
        DataType::Text => "TEXT",
        DataType::Blob => "BLOB",
        DataType::Numeric => "NUMERIC",
    }
}

fn constraint_to_sql(constraint: &Constraint) -> String {
    match constraint {
        Constraint::PrimaryKey => "PRIMARY KEY".to_string(),
        Constraint::NotNull => "NOT NULL".to_string(),
        Constraint::Unique => "UNIQUE".to_string(),
    }
}

/// Generate a random value for a given data type
fn generate_random_value(data_type: &DataType) -> String {
    match data_type {
        DataType::Integer => (get_random() % 1000).to_string(),
        DataType::Real => format!("{:.2}", (get_random() % 1000) as f64 / 100.0),
        DataType::Text => format!("'{}'", generate_random_identifier()),
        DataType::Blob => format!("x'{}'", hex::encode(generate_random_identifier())),
        DataType::Numeric => (get_random() % 1000).to_string(),
    }
}

/// Generate a random INSERT statement for a table
fn generate_insert(table: &Table) -> String {
    let columns = table
        .columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<Vec<_>>()
        .join(", ");

    let values = table
        .columns
        .iter()
        .map(|col| generate_random_value(&col.data_type))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO {} ({}) VALUES ({});",
        table.name, columns, values
    )
}

/// Generate a random UPDATE statement for a table
fn generate_update(table: &Table) -> String {
    // Find the primary key column
    let pk_column = table
        .columns
        .iter()
        .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
        .expect("Table should have a primary key");

    // Get all non-primary key columns
    let non_pk_columns: Vec<_> = table
        .columns
        .iter()
        .filter(|col| col.name != pk_column.name)
        .collect();

    // If we have no non-PK columns, just update the primary key itself
    let set_clause = if non_pk_columns.is_empty() {
        format!(
            "{} = {}",
            pk_column.name,
            generate_random_value(&pk_column.data_type)
        )
    } else {
        non_pk_columns
            .iter()
            .map(|col| format!("{} = {}", col.name, generate_random_value(&col.data_type)))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let where_clause = format!(
        "{} = {}",
        pk_column.name,
        generate_random_value(&pk_column.data_type)
    );

    format!(
        "UPDATE {} SET {} WHERE {};",
        table.name, set_clause, where_clause
    )
}

/// Generate a random DELETE statement for a table
fn generate_delete(table: &Table) -> String {
    // Find the primary key column
    let pk_column = table
        .columns
        .iter()
        .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
        .expect("Table should have a primary key");

    let where_clause = format!(
        "{} = {}",
        pk_column.name,
        generate_random_value(&pk_column.data_type)
    );

    format!("DELETE FROM {} WHERE {};", table.name, where_clause)
}

/// Generate a random SQL statement for a schema
fn generate_random_statement(schema: &ArbitrarySchema) -> String {
    let table = &schema.tables[get_random() as usize % schema.tables.len()];
    match get_random() % 3 {
        0 => generate_insert(table),
        1 => generate_update(table),
        _ => generate_delete(table),
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
            let sql = generate_random_statement(&schema);
            if !opts.skip_log {
                writeln!(log_file, "{}", sql)?;
            }
            queries.push(sql);
        }
        plan.queries_per_thread.push(queries);
    }
    Ok(plan)
}
