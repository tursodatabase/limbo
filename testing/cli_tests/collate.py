#!/usr/bin/env python3
import os
from cli_tests.test_limbo_cli import TestLimboShell
from pydantic import BaseModel
from cli_tests import console


sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


class CollateTest(BaseModel):
    name: str
    db_schema: str = """CREATE TABLE t1(
        x INTEGER PRIMARY KEY,
        a,                 /* collating sequence BINARY */
        b COLLATE BINARY,  /* collating sequence BINARY */
        c COLLATE RTRIM,   /* collating sequence RTRIM  */
        d COLLATE NOCASE   /* collating sequence NOCASE */
    );"""
    db_path: str = "testing/collate.db"

    def init_db(self):
        with TestLimboShell(
            init_commands="",
            exec_name="sqlite3",
            flags=f"{self.db_path}",
        ) as sqlite:
            sqlite.execute_dot(f".open {self.db_path}")
            stmt = [self.db_schema]
            stmt = stmt + [
                "INSERT INTO t1 VALUES(1,'abc','abc', 'abc  ','abc');",
                "INSERT INTO t1 VALUES(2,'abc','abc', 'abc',  'ABC');",
                "INSERT INTO t1 VALUES(3,'abc','abc', 'abc ', 'Abc');",
                "INSERT INTO t1 VALUES(4,'abc','abc ','ABC',  'abc');",
            ]
            stmt.append("SELECT count(*) FROM t1;")

            sqlite.run_test(
                "Init Collate Db in Sqlite",
                "".join(stmt),
                f"{4}",
            )

    def run(self, limbo: TestLimboShell):
        limbo.execute_dot(f".open {self.db_path}")

        limbo.run_test(
            "Text comparison a=b is performed using the BINARY collating sequence",
            "SELECT x FROM t1 WHERE a = b ORDER BY x;",
            "\n".join(map(lambda x: str(x), [1, 2, 3])),
        )

        limbo.run_test(
            "Text comparison a=b is performed using the RTRIM collating sequence",
            "SELECT x FROM t1 WHERE a = b COLLATE RTRIM ORDER BY x;",
            "\n".join(map(lambda x: str(x), [1, 2, 3, 4])),
        )

        limbo.run_test(
            "Text comparison d=a is performed using the NOCASE collating sequence",
            "SELECT x FROM t1 WHERE d = a ORDER BY x;",
            "\n".join(map(lambda x: str(x), [1, 2, 3, 4])),
        )

        limbo.run_test(
            "Text comparison a=d is performed using the BINARY collating sequence.",
            "SELECT x FROM t1 WHERE a = d ORDER BY x;",
            "\n".join(map(lambda x: str(x), [1, 4])),
        )


def cleanup(db_fullpath: str):
    wal_path = f"{db_fullpath}-wal"
    shm_path = f"{db_fullpath}-shm"
    paths = [db_fullpath, wal_path, shm_path]
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def main():
    # Test from using examples from Section 7.2
    # https://sqlite.org/datatype3.html#collation
    test = CollateTest(name="Smoke collate tests")
    console.info(test)

    db_path = test.db_path
    try:
        test.init_db()
        # Use with syntax to automatically close shell on error
        with TestLimboShell("") as limbo:
            test.run(limbo)

        # test.test_compat()
    except Exception as e:
        console.error(f"Test FAILED: {e}")
        cleanup(db_path)
        exit(1)
    # delete db after every compat test so we we have fresh db for next test
    cleanup(db_path)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
