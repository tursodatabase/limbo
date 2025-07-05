#!/usr/bin/env python3
import os
import select
import subprocess
import time
from pathlib import Path
from time import sleep
from typing import Callable, List, Optional

from cli_tests import console

PIPE_BUF = 4096


class ShellConfig:
    def __init__(self, exe_name, flags: str = "-q"):
        self.sqlite_exec: str = exe_name
        self.sqlite_flags: List[str] = flags.split()
        self.cwd = os.getcwd()
        self.test_dir: Path = Path("testing")
        self.py_folder: Path = Path("cli_tests")
        self.test_files: Path = Path("test_files")


class TursoShell:
    def __init__(self, config: ShellConfig, init_commands: Optional[str] = None):
        self.config = config
        self.pipe = self._start_repl(init_commands)

    def _start_repl(self, init_commands: Optional[str]) -> subprocess.Popen:
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        pipe = subprocess.Popen(
            [self.config.sqlite_exec, *self.config.sqlite_flags],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            env=env,
        )
        if init_commands and pipe.stdin is not None:
            pipe.stdin.write((init_commands + "\n").encode())
            pipe.stdin.flush()
        return pipe

    def get_test_filepath(self) -> Path:
        return self.config.test_dir / "limbo_output.txt"

    def execute(self, sql: str) -> str:
        end_marker = "END_OF_RESULT"
        self._write_to_pipe(sql)

        # If we're redirecting output, return so test's don't hang
        if sql.strip().startswith(".output"):
            return ""
        self._write_to_pipe(f"SELECT '{end_marker}';")
        output = ""
        while True:
            ready, _, errors = select.select(
                [self.pipe.stdout, self.pipe.stderr],
                [],
                [self.pipe.stdout, self.pipe.stderr],
            )
            ready_or_errors = set(ready + errors)
            if self.pipe.stderr in ready_or_errors:
                fragment = self.pipe.stderr.read(PIPE_BUF).decode()
                if not fragment:
                    console.error(output, end="", _stack_offset=2)
                    raise RuntimeError("Error encountered in Turso shell.")
                output += fragment
            if self.pipe.stdout in ready_or_errors:
                fragment = self.pipe.stdout.read(PIPE_BUF).decode()
                output += fragment
                if output.rstrip().endswith(end_marker):
                    break
        return self._clean_output(output, end_marker)

    def _write_to_pipe(self, command: str) -> None:
        if not self.pipe.stdin:
            raise RuntimeError("Failed to start Turso REPL")
        self.pipe.stdin.write((command + "\n").encode())
        self.pipe.stdin.flush()

    @staticmethod
    def _clean_output(output: str, marker: str) -> str:
        output = output.rstrip().removesuffix(marker)
        lines = [line.strip() for line in output.split("\n") if line]
        return "\n".join(lines)

    def quit(self) -> None:
        self._write_to_pipe(".quit")
        sleep(0.3)
        self.pipe.terminate()
        self.pipe.kill()


class TestTursoShell:
    def __init__(
        self,
        init_commands: Optional[str] = None,
        init_blobs_table: bool = False,
        exec_name: Optional[str] = None,
        use_testing_db: bool = False,
        flags="",
    ):
        if exec_name is None:
            exec_name = os.environ.get("SQLITE_EXEC", "./scripts/turso-sqlite3")
            if flags == "":
                flags = "-q"
        self.config = ShellConfig(exe_name=exec_name, flags=flags)
        if use_testing_db:
            self.init_test_db()
            init_commands = ".open testing/testing_clone.db"
        if init_commands is None:
            # Default initialization
            init_commands = """
CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER);
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO users VALUES (1, 'Alice', 'Smith', 30), (2, 'Bob', 'Johnson', 25),
                         (3, 'Charlie', 'Brown', 66), (4, 'David', 'Nichols', 70);
INSERT INTO products VALUES (1, 'Hat', 19.99), (2, 'Shirt', 29.99),
                            (3, 'Shorts', 39.99), (4, 'Dress', 49.99);
            """
            if init_blobs_table:
                init_commands += """
CREATE TABLE t (x1, x2, x3, x4);
INSERT INTO t VALUES (zeroblob(1024 - 1), zeroblob(1024 - 2), zeroblob(1024 - 3), zeroblob(1024 - 4));"""

            init_commands += "\n.nullvalue TURSO"
        self.shell = TursoShell(self.config, init_commands)

    def quit(self, cleanup=True):
        if cleanup:
            self.cleanup_test_db()
        self.shell.quit()

    def run_test(self, name: str, sql: str, expected: str) -> None:
        console.test(f"Running test: {name}", _stack_offset=2)
        actual = self.shell.execute(sql)
        assert (
            actual == expected
        ), f"Test failed: {name}\nSQL: {sql}\nExpected:\n{repr(expected)}\nActual:\n{repr(actual)}"

    def run_debug(self, sql: str):
        console.debug(f"debugging: {sql}", _stack_offset=2)
        actual = self.shell.execute(sql)
        console.debug(f"OUTPUT:\n{repr(actual)}", _stack_offset=2)

    def run_test_fn(self, sql: str, validate: Callable[[str], bool], desc: str = "") -> None:
        # Print the test that is executing before executing the sql command
        # Printing later confuses the user of the code what test has actually failed
        if desc:
            console.test(f"Testing: {desc}", _stack_offset=2)
        actual = self.shell.execute(sql)
        assert validate(actual), f"Test failed\nSQL: {sql}\nActual:\n{repr(actual)}"

    def execute_dot(self, dot_command: str) -> None:
        self.shell._write_to_pipe(dot_command)

    def init_test_db(self) -> None:
        self.cleanup_test_db()
        path = os.path.join("testing", "testing_clone.db")
        if os.path.exists(path):
            os.remove(path)
        time.sleep(0.1)  # Ensure the file is removed before cloning
        cmd = "sqlite3 testing/testing.db '.clone testing/testing_clone.db'"
        subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if not os.path.exists("testing/testing_clone.db"):
            raise RuntimeError("Failed to clone testing.db to testing/testing_clone.db")

    def cleanup_test_db(self) -> None:
        path = os.path.join("testing", "testing_clone.db")
        if os.path.exists(path):
            os.remove(path)
        walpath = os.path.join("testing", "testing.db-wal")
        if os.path.exists(walpath):
            os.remove(walpath)

    # Enables the use of `with` syntax
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.cleanup_test_db()
        self.quit()
