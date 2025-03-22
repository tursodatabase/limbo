#!/usr/bin/env python3
from test_limbo_cli import TestLimboShell
from pathlib import Path
import time
import os


def test_basic_queries():
    shell = TestLimboShell()
    shell.run_test("select-1", "SELECT 1;", "1")
    shell.run_test("select-avg", "SELECT avg(age) FROM users;", "47.75")
    shell.run_test("select-sum", "SELECT sum(age) FROM users;", "191")
    shell.run_test("mem-sum-zero", "SELECT sum(first_name) FROM users;", "0.0")
    shell.run_test("mem-total-age", "SELECT total(age) FROM users;", "191.0")
    shell.run_test("mem-typeof", "SELECT typeof(id) FROM users LIMIT 1;", "integer")
    shell.quit()


def test_schema_operations():
    shell = TestLimboShell(init_blobs_table=True)
    expected = (
        "CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER);\n"
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);\n"
        "CREATE TABLE t (x1, x2, x3, x4);"
    )
    shell.run_test("schema-memory", ".schema", expected)
    shell.quit()


def test_file_operations():
    shell = TestLimboShell()
    shell.run_test("file-open", ".open testing/testing.db", "")
    shell.run_test("file-users-count", "select count(*) from users;", "10000")
    shell.quit()

    shell = TestLimboShell()
    shell.run_test("file-schema-1", ".open testing/testing.db", "")
    expected_user_schema = (
        "CREATE TABLE users (\n"
        "id INTEGER PRIMARY KEY,\n"
        "first_name TEXT,\n"
        "last_name TEXT,\n"
        "email TEXT,\n"
        "phone_number TEXT,\n"
        "address TEXT,\n"
        "city TEXT,\n"
        "state TEXT,\n"
        "zipcode TEXT,\n"
        "age INTEGER\n"
        ");\n"
        "CREATE INDEX age_idx on users (age);"
    )
    shell.run_test("file-schema-users", ".schema users", expected_user_schema)
    shell.quit()


def test_joins():
    shell = TestLimboShell()
    shell.run_test("open-file", ".open testing/testing.db", "")
    shell.run_test("verify-tables", ".tables", "counter products users")
    shell.run_test(
        "file-cross-join",
        "select * from users, products limit 1;",
        "1|Jamie|Foster|dylan00@example.com|496-522-9493|62375 Johnson Rest Suite 322|West Lauriestad|IL|35865|94|1|hat|79.0",
    )
    shell.quit()


def test_left_join_self():
    shell = TestLimboShell(
        init_commands="""
    .open testing/testing.db
    """
    )

    shell.run_test(
        "file-left-join-self",
        "select u1.first_name as user_name, u2.first_name as neighbor_name from users u1 left join users as u2 on u1.id = u2.id + 1 limit 2;",
        "Jamie|\nCindy|Jamie",
    )
    shell.quit()


def test_where_clauses():
    shell = TestLimboShell()
    shell.run_test("open-testing-db-file", ".open testing/testing.db", "")
    shell.run_test(
        "where-clause-eq-string",
        "select count(1) from users where last_name = 'Rodriguez';",
        "61",
    )
    shell.quit()


def test_switch_back_to_in_memory():
    shell = TestLimboShell()
    # First, open the file-based DB.
    shell.run_test("open-testing-db-file", ".open testing/testing.db", "")
    # Then switch back to :memory:
    shell.run_test("switch-back", ".open :memory:", "")
    shell.run_test(
        "schema-in-memory", ".schema users", "-- Error: Table 'users' not found."
    )
    shell.quit()


def test_verify_null_value():
    shell = TestLimboShell()
    shell.run_test("verify-null", "select NULL;", "LIMBO")
    shell.quit()


def verify_output_file(filepath: Path, expected_lines: dict) -> None:
    with open(filepath, "r") as f:
        contents = f.read()
    for line, description in expected_lines.items():
        assert line in contents, f"Missing: {description}"


def test_output_file():
    shell = TestLimboShell()
    output_filename = "limbo_output.txt"
    output_file = shell.config.test_dir / shell.config.py_folder / output_filename

    shell.execute_dot(".open testing/testing.db")

    shell.execute_dot(f".cd {shell.config.test_dir}/{shell.config.py_folder}")
    shell.execute_dot(".echo on")
    shell.execute_dot(f".output {output_filename}")
    shell.execute_dot(f".cd {shell.config.test_dir}/{shell.config.py_folder}")
    shell.execute_dot(".mode pretty")
    shell.execute_dot("SELECT 'TEST_ECHO';")
    shell.execute_dot("")
    shell.execute_dot(".echo off")
    shell.execute_dot(".nullvalue LIMBO")
    shell.execute_dot(".show")
    shell.execute_dot(".output stdout")
    time.sleep(3)

    with open(output_file, "r") as f:
        contents = f.read()

    expected_lines = {
        f"Output: {output_filename}": "Can direct output to a file",
        "Output mode: list": "Output mode remains list when output is redirected",
        "Error: pretty output can only be written to a tty": "Error message for pretty mode",
        "SELECT 'TEST_ECHO'": "Echoed command",
        "TEST_ECHO": "Echoed result",
        "Null value: LIMBO": "Null value setting",
        f"CWD: {shell.config.cwd}/{shell.config.test_dir}": "Working directory changed",
        "DB: testing/testing.db": "File database opened",
        "Echo: off": "Echo turned off",
    }

    for line, _ in expected_lines.items():
        assert line in contents, f"Expected line not found in file: {line}"

    # Clean up
    os.remove(output_file)


def test_multi_line_single_line_comments_succession():
    shell = TestLimboShell()
    comments = """-- First of the comments
-- Second line of the comments
SELECT 2;"""
    shell.run_test("multi-line-single-line-comments", comments, "2")
    shell.quit()


def test_comments():
    shell = TestLimboShell()
    shell.run_test("single-line-comment", "-- this is a comment\nSELECT 1;", "1")
    shell.run_test(
        "multi-line-comments", "-- First comment\n-- Second comment\nSELECT 2;", "2"
    )
    shell.run_test("block-comment", "/*\nMulti-line block comment\n*/\nSELECT 3;", "3")
    shell.run_test(
        "inline-comments",
        "SELECT id, -- comment here\nfirst_name FROM users LIMIT 1;",
        "1|Alice",
    )
    shell.quit()


def test_import_csv():
    shell = TestLimboShell()
    shell.run_test("memory-db", ".open :memory:", "")
    shell.run_test(
        "create-csv-table", "CREATE TABLE csv_table (c1 INT, c2 REAL, c3 String);", ""
    )
    shell.run_test(
        "import-csv-no-options",
        ".import --csv ./testing/test_files/test.csv csv_table",
        "",
    )
    shell.run_test(
        "verify-csv-no-options",
        "select * from csv_table;",
        "1|2.0|String'1\n3|4.0|String2",
    )
    shell.quit()


def test_import_csv_verbose():
    shell = TestLimboShell()
    shell.run_test("open-memory", ".open :memory:", "")
    shell.run_test(
        "create-csv-table", "CREATE TABLE csv_table (c1 INT, c2 REAL, c3 String);", ""
    )
    shell.run_test(
        "import-csv-verbose",
        ".import --csv -v ./testing/test_files/test.csv csv_table",
        "Added 2 rows with 0 errors using 2 lines of input",
    )
    shell.run_test(
        "verify-csv-verbose",
        "select * from csv_table;",
        "1|2.0|String'1\n3|4.0|String2",
    )
    shell.quit()


def test_import_csv_skip():
    shell = TestLimboShell()
    shell.run_test("open-memory", ".open :memory:", "")
    shell.run_test(
        "create-csv-table", "CREATE TABLE csv_table (c1 INT, c2 REAL, c3 String);", ""
    )
    shell.run_test(
        "import-csv-skip",
        ".import --csv --skip 1 ./testing/test_files/test.csv csv_table",
        "",
    )
    shell.run_test("verify-csv-skip", "select * from csv_table;", "3|4.0|String2")
    shell.quit()


def test_table_patterns():
    shell = TestLimboShell()
    shell.run_test("tables-pattern", ".tables us%", "users")
    shell.quit()


if __name__ == "__main__":
    print("Running all Limbo CLI tests...")
    test_basic_queries()
    test_schema_operations()
    test_file_operations()
    test_joins()
    test_left_join_self()
    test_where_clauses()
    test_switch_back_to_in_memory()
    test_verify_null_value()
    test_output_file()
    test_multi_line_single_line_comments_succession()
    test_comments()
    test_import_csv()
    test_import_csv_verbose()
    test_import_csv_skip()
    test_table_patterns()
    print("All tests have passed")
