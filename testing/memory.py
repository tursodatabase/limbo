#!/usr/bin/env python3
import os
import subprocess
import select


sqlite_exec = "./target/debug/limbo"
sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


def init_limbo():
    pipe = subprocess.Popen(
        [sqlite_exec, *sqlite_flags],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    return pipe


def create_new_limbo():
    pipe = init_limbo()
    return pipe


def execute_sql(pipe, sql):
    end_suffix = "END_OF_RESULT"
    write_to_pipe(pipe, sql)
    write_to_pipe(pipe, f"SELECT '{end_suffix}';\n")
    stdout = pipe.stdout
    stderr = pipe.stderr
    output = ""
    while True:
        ready_to_read, _, error_in_pipe = select.select(
            [stdout, stderr], [], [stdout, stderr]
        )
        ready_to_read_or_err = set(ready_to_read + error_in_pipe)
        if stderr in ready_to_read_or_err:
            exit_on_error(stderr)

        if stdout in ready_to_read_or_err:
            fragment = stdout.read(select.PIPE_BUF)
            output += fragment.decode()
            if output.rstrip().endswith(end_suffix):
                output = output.rstrip().removesuffix(end_suffix)
                break
    output = strip_each_line(output)
    return output


def strip_each_line(lines: str) -> str:
    split = lines.split("\n")
    res = [line.strip() for line in split if line != ""]
    return "\n".join(res)


def write_to_pipe(pipe, command):
    if pipe.stdin is None:
        raise RuntimeError("Failed to write to shell")
    pipe.stdin.write((command + "\n").encode())
    pipe.stdin.flush()


def exit_on_error(stderr):
    while True:
        ready_to_read, _, _ = select.select([stderr], [], [])
        if not ready_to_read:
            break
        print(stderr.read().decode(), end="")
    exit(1)


def run_test(pipe, sql, validator=None, name=None):
    print(f"Running test {name}")
    result = execute_sql(pipe, sql)
    if validator is not None:
        (condition, expected) = validator(result)
        if not condition:
            print(f"Test FAILED: {sql}")
            print(f"Expected: {expected}")
            print(f"Returned: {result}")
            raise Exception("Validation failed")
    print("Test PASSED")


def validate_true(result):
    return result == "1"


def validate_false(result):
    return result == "0"


def validate_blob(result):
    # HACK: blobs are difficult to test because the shell
    # tries to return them as utf8 strings, so we call hex
    # and assert they are valid hex digits
    return int(result, 16) is not None


def returns_null(result):
    return result == "" or result == "\n"


def validate_with_expected(result: str, expected: str):
    return (expected in result, expected)


def stub_memory_test(
    pipe: subprocess.Popen[bytes],
    name: str,
    blob_size: int = 1024**2,
    vals: int = 100,
    blobs: bool = True,
):
    # zero_blob_size = 1024 **2
    zero_blob = "0" * blob_size * 2
    # vals = 100
    big_stmt = ["CREATE TABLE temp (t1 BLOB, t2 INTEGER);"]
    big_stmt = big_stmt + [
        f"INSERT INTO temp (t1) VALUES (zeroblob({blob_size}));"
        if i % 2 == 0 and blobs
        else f"INSERT INTO temp (t2) VALUES ({i});"
        for i in range(vals * 2)
    ]
    expected = []
    for i in range(vals * 2):
        if i % 2 == 0 and blobs:
            big_stmt.append(f"SELECT hex(t1) FROM temp LIMIT 1 OFFSET {i};")
            expected.append(zero_blob)
        else:
            big_stmt.append(f"SELECT t2 FROM temp LIMIT 1 OFFSET {i};")
            expected.append(f"{i}")

    big_stmt.append("SELECT count(*) FROM temp;")
    expected.append(str(vals * 2))

    big_stmt = "".join(big_stmt)
    expected = "\n".join(expected)

    run_test(
        pipe,
        big_stmt,
        lambda res: validate_with_expected(res, expected),
        name,
    )


# TODO no delete tests for now because of limbo outputs some debug information on delete
def memory_tests() -> list[dict]:
    tests = []

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


def main():
    tests = memory_tests()
    # TODO see how to parallelize this loop with different subprocesses
    for test in tests:
        pipe = init_limbo()
        with pipe:
            try:
                stub_memory_test(pipe, **test)

            except Exception as e:
                print(f"Test FAILED: {e}")
                pipe.terminate()
                exit(1)
    print("All tests passed successfully.")


if __name__ == "__main__":
    main()
