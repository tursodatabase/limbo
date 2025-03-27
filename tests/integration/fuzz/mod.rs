pub mod grammar_generator;

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    use crate::{
        common::TempDatabase,
        fuzz::grammar_generator::{const_str, rand_int, rand_str, GrammarGenerator},
    };

    use super::grammar_generator::SymbolHandle;

    fn rng_from_time() -> (ChaCha8Rng, u64) {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    }

    fn sqlite_exec_rows(
        conn: &rusqlite::Connection,
        query: &str,
    ) -> Vec<Vec<rusqlite::types::Value>> {
        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(params![]).unwrap();
        let mut results = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let mut result = Vec::new();
            for i in 0.. {
                let column: rusqlite::types::Value = match row.get(i) {
                    Ok(column) => column,
                    Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                    Err(err) => panic!("unexpected rusqlite error: {}", err),
                };
                result.push(column);
            }
            results.push(result)
        }

        results
    }

    fn limbo_exec_rows(
        db: &TempDatabase,
        conn: &Rc<limbo_core::Connection>,
        query: &str,
    ) -> Vec<Vec<rusqlite::types::Value>> {
        let mut stmt = conn.prepare(query).unwrap();
        let mut rows = Vec::new();
        'outer: loop {
            let row = loop {
                let result = stmt.step().unwrap();
                match result {
                    limbo_core::StepResult::Row => {
                        let row = stmt.row().unwrap();
                        break row;
                    }
                    limbo_core::StepResult::IO => {
                        db.io.run_once().unwrap();
                        continue;
                    }
                    limbo_core::StepResult::Done => break 'outer,
                    r => panic!("unexpected result {:?}: expecting single row", r),
                }
            };
            let row = row
                .get_values()
                .iter()
                .map(|x| match x {
                    limbo_core::RefValue::Null => rusqlite::types::Value::Null,
                    limbo_core::RefValue::Integer(x) => rusqlite::types::Value::Integer(*x),
                    limbo_core::RefValue::Float(x) => rusqlite::types::Value::Real(*x),
                    limbo_core::RefValue::Text(x) => {
                        rusqlite::types::Value::Text(x.as_str().to_string())
                    }
                    limbo_core::RefValue::Blob(x) => {
                        rusqlite::types::Value::Blob(x.to_slice().to_vec())
                    }
                })
                .collect();
            rows.push(row);
        }
        rows
    }

    #[test]
    pub fn arithmetic_expression_fuzz_ex1() {
        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        for query in [
            "SELECT ~1 >> 1536",
            "SELECT ~ + 3 << - ~ (~ (8)) - + -1 - 3 >> 3 + -6 * (-7 * 9 >> - 2)",
        ] {
            let limbo = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?}",
                query, limbo, sqlite
            );
        }
    }

    #[test]
    pub fn arithmetic_expression_fuzz() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (unary_op, unary_op_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        unary_op_builder
            .concat(" ")
            .push(g.create().choice().options_str(["~", "+", "-"]).build())
            .push(expr)
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["+", "-", "*", "/", "%", "&", "|", "<<", ">>"])
                    .build(),
            )
            .push(expr)
            .build();

        expr_builder
            .choice()
            .option_w(unary_op, 1.0)
            .option_w(bin_op, 1.0)
            .option_w(paren, 1.0)
            .option_symbol_w(rand_int(-10..10), 1.0)
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {}", seed);
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?} seed: {}",
                query, limbo, sqlite, seed
            );
        }
    }

    #[test]
    pub fn fuzz_ex() {
        let _ = env_logger::try_init();
        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        for query in [
            "SELECT FALSE",
            "SELECT NOT FALSE",
            "SELECT ((NULL) IS NOT TRUE <= ((NOT (FALSE))))",
            "SELECT ifnull(0, NOT 0)",
            "SELECT like('a%', 'a') = 1",
            "SELECT CASE ( NULL < NULL ) WHEN ( 0 ) THEN ( NULL ) ELSE ( 2.0 ) END;",
            "SELECT (COALESCE(0, COALESCE(0, 0)));",
            "SELECT CAST((1 > 0) AS INTEGER);",
            "SELECT substr('ABC', -1)",
        ] {
            let limbo = limbo_exec_rows(&db, &limbo_conn, query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?}",
                query, limbo, sqlite
            );
        }
    }

    #[test]
    pub fn math_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (scalar, scalar_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["+", "-", "/", "*"])
                    .build(),
            )
            .push(expr)
            .build();

        scalar_builder
            .choice()
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str([
                                "acos", "acosh", "asin", "asinh", "atan", "atanh", "ceil",
                                "ceiling", "cos", "cosh", "degrees", "exp", "floor", "ln", "log",
                                "log10", "log2", "radians", "sin", "sinh", "sqrt", "tan", "tanh",
                                "trunc",
                            ])
                            .build(),
                    )
                    .push_str("(")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str(["atan2", "log", "mod", "pow", "power"])
                            .build(),
                    )
                    .push_str("(")
                    .push(g.create().concat("").push(expr).repeat(2..3, ", ").build())
                    .push_str(")")
                    .build(),
            )
            .build();

        expr_builder
            .choice()
            .options_str(["-2.0", "-1.0", "0.0", "0.5", "1.0", "2.0"])
            .option_w(bin_op, 10.0)
            .option_w(paren, 10.0)
            .option_w(scalar, 10.0)
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {}", seed);
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {}", query);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            match (&limbo[0][0], &sqlite[0][0]) {
                // compare only finite results because some evaluations are not so stable around infinity
                (rusqlite::types::Value::Real(limbo), rusqlite::types::Value::Real(sqlite))
                    if limbo.is_finite() && sqlite.is_finite() =>
                {
                    assert!(
                        (limbo - sqlite).abs() < 1e-9
                            || (limbo - sqlite) / (limbo.abs().max(sqlite.abs())) < 1e-9,
                        "query: {}, limbo: {:?}, sqlite: {:?} seed: {}",
                        query,
                        limbo,
                        sqlite,
                        seed
                    )
                }
                _ => {}
            }
        }
    }

    #[test]
    pub fn string_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (scalar, scalar_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();
        let (number, number_builder) = g.create_handle();

        number_builder
            .choice()
            .option_symbol(rand_int(-5..10))
            .option(
                g.create()
                    .concat(" ")
                    .push(number)
                    .push(g.create().choice().options_str(["+", "-", "*"]).build())
                    .push(number)
                    .build(),
            )
            .build();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(g.create().choice().options_str(["||"]).build())
            .push(expr)
            .build();

        scalar_builder
            .choice()
            .option(
                g.create()
                    .concat("")
                    .push_str("char(")
                    .push(
                        g.create()
                            .concat("")
                            .push_symbol(rand_int(65..91))
                            .repeat(1..8, ", ")
                            .build(),
                    )
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str(["ltrim", "rtrim", "trim"])
                            .build(),
                    )
                    .push_str("(")
                    .push(g.create().concat("").push(expr).repeat(2..3, ", ").build())
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str([
                                "ltrim", "rtrim", "lower", "upper", "quote", "hex", "trim",
                            ])
                            .build(),
                    )
                    .push_str("(")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(g.create().choice().options_str(["replace"]).build())
                    .push_str("(")
                    .push(g.create().concat("").push(expr).repeat(3..4, ", ").build())
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push(
                        g.create()
                            .choice()
                            .options_str(["substr", "substring"])
                            .build(),
                    )
                    .push_str("(")
                    .push(expr)
                    .push_str(", ")
                    .push(
                        g.create()
                            .concat("")
                            .push(number)
                            .repeat(1..3, ", ")
                            .build(),
                    )
                    .push_str(")")
                    .build(),
            )
            .build();

        expr_builder
            .choice()
            .option_w(bin_op, 1.0)
            .option_w(paren, 1.0)
            .option_w(scalar, 1.0)
            .option(
                g.create()
                    .concat("")
                    .push_str("'")
                    .push_symbol(rand_str("", 2))
                    .push_str("'")
                    .build(),
            )
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {}", seed);
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {}", query);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?} seed: {}",
                query, limbo, sqlite, seed
            );
        }
    }

    struct TestTable {
        pub name: &'static str,
        pub columns: Vec<&'static str>,
    }

    /// Expressions that can be used in both SELECT and WHERE positions.
    struct CommonBuilders {
        pub bin_op: SymbolHandle,
        pub unary_infix_op: SymbolHandle,
        pub scalar: SymbolHandle,
        pub paren: SymbolHandle,
        pub coalesce_expr: SymbolHandle,
        pub cast_expr: SymbolHandle,
        pub case_expr: SymbolHandle,
        pub cmp_op: SymbolHandle,
        pub number: SymbolHandle,
    }

    /// Expressions that can be used only in WHERE position due to Limbo limitations.
    struct PredicateBuilders {
        pub in_op: SymbolHandle,
    }

    fn common_builders(g: &GrammarGenerator, tables: Option<&[TestTable]>) -> CommonBuilders {
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (unary_infix_op, unary_infix_op_builder) = g.create_handle();
        let (scalar, scalar_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();
        let (like_pattern, like_pattern_builder) = g.create_handle();
        let (glob_pattern, glob_pattern_builder) = g.create_handle();
        let (coalesce_expr, coalesce_expr_builder) = g.create_handle();
        let (cast_expr, cast_expr_builder) = g.create_handle();
        let (case_expr, case_expr_builder) = g.create_handle();
        let (cmp_op, cmp_op_builder) = g.create_handle();
        let (column, column_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        unary_infix_op_builder
            .concat(" ")
            .push(g.create().choice().options_str(["NOT"]).build())
            .push(expr)
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["AND", "OR", "IS", "IS NOT", "=", "<>", ">", "<", ">=", "<="])
                    .build(),
            )
            .push(expr)
            .build();

        like_pattern_builder
            .choice()
            .option_str("%")
            .option_str("_")
            .option_symbol(rand_str("", 1))
            .repeat(1..10, "")
            .build();

        glob_pattern_builder
            .choice()
            .option_str("*")
            .option_str("**")
            .option_str("A")
            .option_str("B")
            .repeat(1..10, "")
            .build();

        coalesce_expr_builder
            .concat("")
            .push_str("COALESCE(")
            .push(g.create().concat("").push(expr).repeat(2..5, ",").build())
            .push_str(")")
            .build();

        cast_expr_builder
            .concat(" ")
            .push_str("CAST ( (")
            .push(expr)
            .push_str(") AS ")
            // cast to INTEGER/REAL/TEXT types can be added when Limbo will use proper equality semantic between values (e.g. 1 = 1.0)
            .push(g.create().choice().options_str(["NUMERIC"]).build())
            .push_str(")")
            .build();

        case_expr_builder
            .concat(" ")
            .push_str("CASE (")
            .push(expr)
            .push_str(")")
            .push(
                g.create()
                    .concat(" ")
                    .push_str("WHEN (")
                    .push(expr)
                    .push_str(") THEN (")
                    .push(expr)
                    .push_str(")")
                    .repeat(1..5, " ")
                    .build(),
            )
            .push_str("ELSE (")
            .push(expr)
            .push_str(") END")
            .build();

        scalar_builder
            .choice()
            .option(coalesce_expr)
            .option(
                g.create()
                    .concat("")
                    .push_str("like('")
                    .push(like_pattern)
                    .push_str("', '")
                    .push(like_pattern)
                    .push_str("')")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push_str("glob('")
                    .push(glob_pattern)
                    .push_str("', '")
                    .push(glob_pattern)
                    .push_str("')")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push_str("ifnull(")
                    .push(expr)
                    .push_str(",")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .option(
                g.create()
                    .concat("")
                    .push_str("iif(")
                    .push(expr)
                    .push_str(",")
                    .push(expr)
                    .push_str(",")
                    .push(expr)
                    .push_str(")")
                    .build(),
            )
            .build();

        let number = g
            .create()
            .choice()
            .option_symbol(rand_int(-0xff..0x100))
            .option_symbol(rand_int(-0xffff..0x10000))
            .option_symbol(rand_int(-0xffffff..0x1000000))
            .option_symbol(rand_int(-0xffffffff..0x100000000))
            .option_symbol(rand_int(-0xffffffffffff..0x1000000000000))
            .build();

        let mut column_builder = column_builder
            .choice()
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push_str(")")
                    .build(),
            )
            .option(number)
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push(
                        g.create()
                            .choice()
                            .options_str([
                                "+", "-", "*", "/", "||", "=", "<>", ">", "<", ">=", "<=", "IS",
                                "IS NOT",
                            ])
                            .build(),
                    )
                    .push(column)
                    .push_str(")")
                    .build(),
            );

        if let Some(tables) = tables {
            for table in tables.iter() {
                for column in table.columns.iter() {
                    column_builder = column_builder
                        .option_symbol_w(const_str(&format!("{}.{}", table.name, column)), 1.0);
                }
            }
        }

        column_builder.build();

        cmp_op_builder
            .concat(" ")
            .push(column)
            .push(
                g.create()
                    .choice()
                    .options_str(["=", "<>", ">", "<", ">=", "<=", "IS", "IS NOT"])
                    .build(),
            )
            .push(column)
            .build();

        expr_builder
            .choice()
            .option_w(bin_op, 3.0)
            .option_w(unary_infix_op, 2.0)
            .option_w(paren, 2.0)
            .option_w(scalar, 4.0)
            .option_w(coalesce_expr, 1.0)
            .option_w(cast_expr, 1.0)
            .option_w(case_expr, 1.0)
            .option_w(cmp_op, 1.0)
            .options_str(["1", "0", "NULL", "2.0", "1.5", "-0.5", "-2.0", "(1 / 0)"])
            .build();

        CommonBuilders {
            bin_op,
            unary_infix_op,
            scalar,
            paren,
            coalesce_expr,
            cast_expr,
            case_expr,
            cmp_op,
            number,
        }
    }

    fn predicate_builders(g: &GrammarGenerator, tables: Option<&[TestTable]>) -> PredicateBuilders {
        let (in_op, in_op_builder) = g.create_handle();
        let (column, column_builder) = g.create_handle();
        let mut column_builder = column_builder
            .choice()
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push_str(")")
                    .build(),
            )
            .option_symbol(rand_int(-0xffffffff..0x100000000))
            .option(
                g.create()
                    .concat(" ")
                    .push_str("(")
                    .push(column)
                    .push(g.create().choice().options_str(["+", "-"]).build())
                    .push(column)
                    .push_str(")")
                    .build(),
            );

        if let Some(tables) = tables {
            for table in tables.iter() {
                for column in table.columns.iter() {
                    column_builder = column_builder
                        .option_symbol_w(const_str(&format!("{}.{}", table.name, column)), 1.0);
                }
            }
        }

        column_builder.build();

        in_op_builder
            .concat(" ")
            .push(column)
            .push(g.create().choice().options_str(["IN", "NOT IN"]).build())
            .push_str("(")
            .push(
                g.create()
                    .concat("")
                    .push(column)
                    .repeat(1..5, ", ")
                    .build(),
            )
            .push_str(")")
            .build();

        PredicateBuilders { in_op }
    }

    fn build_logical_expr(
        g: &GrammarGenerator,
        common: &CommonBuilders,
        predicate: Option<&PredicateBuilders>,
    ) -> SymbolHandle {
        let (handle, builder) = g.create_handle();
        let mut builder = builder
            .choice()
            .option_w(common.cast_expr, 1.0)
            .option_w(common.case_expr, 1.0)
            .option_w(common.cmp_op, 1.0)
            .option_w(common.coalesce_expr, 1.0)
            .option_w(common.unary_infix_op, 2.0)
            .option_w(common.bin_op, 3.0)
            .option_w(common.paren, 2.0)
            .option_w(common.scalar, 4.0)
            // unfortunately, sqlite behaves weirdly when IS operator is used with TRUE/FALSE constants
            // e.g. 8 IS TRUE == 1 (although 8 = TRUE == 0)
            // so, we do not use TRUE/FALSE constants as they will produce diff with sqlite results
            .options_str(["1", "0", "NULL", "2.0", "1.5", "-0.5", "-2.0", "(1 / 0)"]);

        if let Some(predicate) = predicate {
            builder = builder.option_w(predicate.in_op, 1.0);
        }

        builder.build();

        handle
    }

    #[test]
    pub fn logical_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let builders = common_builders(&g, None);
        let expr = build_logical_expr(&g, &builders, None);

        let sql = g
            .create()
            .concat(" ")
            .push_str("SELECT ")
            .push(expr)
            .build();

        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {}", seed);
        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {}", query);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?} seed: {}",
                query, limbo, sqlite, seed
            );
        }
    }

    #[test]
    pub fn table_logical_expression_fuzz_ex1() {
        let _ = env_logger::try_init();

        for queries in [
            [
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (10)",
                "SELECT * FROM t WHERE  x = 1 AND 1 OR 0",
            ],
            [
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (-3258184727)",
                "SELECT * FROM t",
            ],
        ] {
            let db = TempDatabase::new_empty();
            let limbo_conn = db.connect_limbo();
            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            for query in queries.iter() {
                let limbo = limbo_exec_rows(&db, &limbo_conn, query);
                let sqlite = sqlite_exec_rows(&sqlite_conn, query);
                assert_eq!(
                    limbo, sqlite,
                    "queries: {:?}, query: {}, limbo: {:?}, sqlite: {:?}",
                    queries, query, limbo, sqlite
                );
            }
        }
    }

    #[test]
    pub fn table_logical_expression_fuzz_run() {
        let _ = env_logger::try_init();
        let g = GrammarGenerator::new();
        let tables = vec![TestTable {
            name: "t",
            columns: vec!["x", "y", "z"],
        }];
        let builders = common_builders(&g, Some(&tables));
        let predicate = predicate_builders(&g, Some(&tables));
        let expr = build_logical_expr(&g, &builders, Some(&predicate));

        let db = TempDatabase::new_empty();
        let limbo_conn = db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        for table in tables.iter() {
            let query = format!("CREATE TABLE {} ({})", table.name, table.columns.join(", "));
            assert_eq!(
                limbo_exec_rows(&db, &limbo_conn, &query),
                sqlite_exec_rows(&sqlite_conn, &query)
            );
        }

        let (mut rng, seed) = rng_from_time();
        log::info!("seed: {}", seed);

        for _ in 0..100 {
            let (x, y, z) = (
                g.generate(&mut rng, builders.number, 1),
                g.generate(&mut rng, builders.number, 1),
                g.generate(&mut rng, builders.number, 1),
            );
            let query = format!("INSERT INTO t VALUES ({}, {}, {})", x, y, z);
            log::info!("insert: {}", query);
            assert_eq!(
                limbo_exec_rows(&db, &limbo_conn, &query),
                sqlite_exec_rows(&sqlite_conn, &query),
                "seed: {}",
                seed,
            );
        }

        let sql = g
            .create()
            .concat(" ")
            .push_str("SELECT * FROM t WHERE ")
            .push(expr)
            .build();

        for _ in 0..1024 {
            let query = g.generate(&mut rng, sql, 50);
            log::info!("query: {}", query);
            let limbo = limbo_exec_rows(&db, &limbo_conn, &query);
            let sqlite = sqlite_exec_rows(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?} seed: {}",
                query, limbo, sqlite, seed
            );
        }
    }
}
