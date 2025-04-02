#!/usr/bin/env python3
import os
from test_limbo_cli import TestLimboShell


sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


def validate_with_expected(result: str, expected: str):
    return (expected in result, expected)


def stub_write_blob_test(
    limbo: TestLimboShell,
    name: str,
    blob_size: int = 1024**2,
    vals: int = 100,
    blobs: bool = True,
    schema: str = "CREATE TABLE test (t1 BLOB, t2 INTEGER);",
):
    zero_blob = "0" * blob_size * 2
    big_stmt = [schema]
    big_stmt = big_stmt + [
        f"INSERT INTO test (t1) VALUES (zeroblob({blob_size}));"
        if i % 2 == 0 and blobs
        else f"INSERT INTO test (t2) VALUES ({i});"
        for i in range(vals * 2)
    ]
    expected = []
    for i in range(vals * 2):
        if i % 2 == 0 and blobs:
            big_stmt.append(f"SELECT hex(t1) FROM test LIMIT 1 OFFSET {i};")
            expected.append(zero_blob)
        else:
            big_stmt.append(f"SELECT t2 FROM test LIMIT 1 OFFSET {i};")
            expected.append(f"{i}")

    big_stmt.append("SELECT count(*) FROM test;")
    expected.append(str(vals * 2))

    big_stmt = "".join(big_stmt)
    expected = "\n".join(expected)

    limbo.run_test_fn(big_stmt, lambda res: validate_with_expected(res, expected), name)


# TODO no delete tests for now
def blob_tests() -> list[dict]:
    tests: list[dict] = []

    for vals in range(0, 1000, 100):
        tests.append(
            {
                "name": f"small-insert-integer-vals-{vals}",
                "vals": vals,
                "blobs": False,
            }
        )

    tests.append(
        {
            "name": f"small-insert-blob-interleaved-blob-size-{1024}",
            "vals": 10,
            "blob_size": 1024,
        }
    )
    tests.append(
        {
            "name": f"big-insert-blob-interleaved-blob-size-{1024}",
            "vals": 100,
            "blob_size": 1024,
        }
    )

    for blob_size in range(0, (1024 * 1024) + 1, 1024 * 4**4):
        if blob_size == 0:
            continue
        tests.append(
            {
                "name": f"small-insert-blob-interleaved-blob-size-{blob_size}",
                "vals": 10,
                "blob_size": blob_size,
            }
        )
        tests.append(
            {
                "name": f"big-insert-blob-interleaved-blob-size-{blob_size}",
                "vals": 100,
                "blob_size": blob_size,
            }
        )
    return tests


def test_sqlite_compat(db_fullpath: str, schema: str):
    sqlite = TestLimboShell(
        init_commands="",
        exec_name="sqlite3",
        flags=f"{db_fullpath}",
    )
    sqlite.run_test_fn(
        ".show",
        lambda res: f"filename: {db_fullpath}" in res,
        "Opened db file created with Limbo in sqlite3",
    )
    sqlite.run_test_fn(
        ".schema",
        lambda res: schema in res,
        "Tables created by previous Limbo test exist in db file",
    )
    # TODO when we can import external dependencies
    # Have some pydantic object be passed to this function with common fields
    # To extract the information necessary to query the db in sqlite
    # The object should contain Schema information and queries that should be run to
    # test in sqlite for compatibility sakes

    # sqlite.run_test_fn(
    #     "SELECT count(*) FROM test;",
    #     lambda res: res == "50",
    #     "Tested large write to testfs",
    # )
    # sqlite.run_test_fn(
    #     "SELECT count(*) FROM vfs;",
    #     lambda res: res == "50",
    #     "Tested large write to testfs",
    # )
    sqlite.quit()


def touch_db_file(db_fullpath: str):
    os.O_RDWR
    descriptor = os.open(
        path=db_fullpath,
        flags=(
            os.O_RDWR  # access mode: read and write
            | os.O_CREAT  # create if not exists
            | os.O_TRUNC  # truncate the file to zero
        ),
        mode=0o777,
    )
    f = open(descriptor)
    f.close()


def cleanup(db_fullpath: str):
    wal_path = f"{db_fullpath}-wal"
    shm_path = f"{db_fullpath}-shm"
    paths = [db_fullpath, wal_path, shm_path]
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    tests = blob_tests()
    db_path = "testing/writes.db"
    schema = "CREATE TABLE test (t1 BLOB, t2 INTEGER);"
    # TODO see how to parallelize this loop with different subprocesses
    for test in tests:
        try:
            # Use with syntax to automatically close shell on error
            with TestLimboShell() as limbo:
                limbo.execute_dot(f".open {db_path}")
                stub_write_blob_test(limbo, **test)
            print("Testing in SQLite\n")
            test_sqlite_compat(db_path, schema)
            print()

        except Exception as e:
            print(f"Test FAILED: {e}")
            cleanup(db_path)
            exit(1)
        # delete db after every compat test so we we have fresh db for next test
        cleanup(db_path)
    print("All tests passed successfully.")
