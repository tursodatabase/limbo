#!/usr/bin/env python3
import os
import subprocess
import select
import time

sqlite_exec = "./target/debug/limbo"
sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")

test_data = """CREATE TABLE numbers ( id INTEGER PRIMARY KEY, value FLOAT NOT NULL);
INSERT INTO numbers (value) VALUES (1.0);
INSERT INTO numbers (value) VALUES (2.0);
INSERT INTO numbers (value) VALUES (3.0);
INSERT INTO numbers (value) VALUES (4.0);
INSERT INTO numbers (value) VALUES (5.0);
INSERT INTO numbers (value) VALUES (6.0);
INSERT INTO numbers (value) VALUES (7.0);
CREATE TABLE test (value REAL, percent REAL);
INSERT INTO test values (10, 25);
INSERT INTO test values (20, 25);
INSERT INTO test values (30, 25);
INSERT INTO test values (40, 25);
INSERT INTO test values (50, 25);
INSERT INTO test values (60, 25);
INSERT INTO test values (70, 25);
"""


def init_limbo():
    pipe = subprocess.Popen(
        [sqlite_exec, *sqlite_flags],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    write_to_pipe(pipe, test_data)
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
    print(f"Running test {name}: {sql}")
    result = execute_sql(pipe, sql)
    if validator is not None:
        if not validator(result):
            print(f"Test FAILED: {sql}")
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


def validate_string_uuid(result):
    return len(result) == 36 and result.count("-") == 4


def returns_error_no_func(result):
    return "error: no such function: " in result


def returns_vtable_parse_err(result):
    return "Parse error: Virtual table" in result


def returns_null(result):
    return result == "" or result == "\n"


def assert_now_unixtime(result):
    return result == str(int(time.time()))


def assert_specific_time(result):
    return result == "1736720789"


def test_uuid(pipe):
    specific_time = "01945ca0-3189-76c0-9a8f-caf310fc8b8e"
    # these are built into the binary, so we just test they work
    run_test(
        pipe,
        "SELECT hex(uuid4());",
        validate_blob,
        "uuid functions are registered properly with ext loaded",
    )
    run_test(pipe, "SELECT uuid4_str();", validate_string_uuid)
    run_test(pipe, "SELECT hex(uuid7());", validate_blob)
    run_test(
        pipe,
        "SELECT uuid7_timestamp_ms(uuid7()) / 1000;",
    )
    run_test(pipe, "SELECT uuid7_str();", validate_string_uuid)
    run_test(pipe, "SELECT uuid_str(uuid7());", validate_string_uuid)
    run_test(pipe, "SELECT hex(uuid_blob(uuid7_str()));", validate_blob)
    run_test(pipe, "SELECT uuid_str(uuid_blob(uuid7_str()));", validate_string_uuid)
    run_test(
        pipe,
        f"SELECT uuid7_timestamp_ms('{specific_time}') / 1000;",
        assert_specific_time,
    )
    run_test(
        pipe,
        "SELECT gen_random_uuid();",
        validate_string_uuid,
        "scalar alias's are registered properly",
    )


def test_regexp(pipe):
    extension_path = "./target/debug/liblimbo_regexp.so"

    # before extension loads, assert no function
    run_test(pipe, "SELECT regexp('a.c', 'abc');", returns_error_no_func)
    run_test(pipe, f".load {extension_path}", returns_null)
    print(f"Extension {extension_path} loaded successfully.")
    run_test(pipe, "SELECT regexp('a.c', 'abc');", validate_true)
    run_test(pipe, "SELECT regexp('a.c', 'ac');", validate_false)
    run_test(pipe, "SELECT regexp('[0-9]+', 'the year is 2021');", validate_true)
    run_test(pipe, "SELECT regexp('[0-9]+', 'the year is unknow');", validate_false)
    run_test(pipe, "SELECT regexp_like('the year is 2021', '[0-9]+');", validate_true)
    run_test(
        pipe, "SELECT regexp_like('the year is unknow', '[0-9]+');", validate_false
    )
    run_test(
        pipe,
        "SELECT regexp_substr('the year is 2021', '[0-9]+') = '2021';",
        validate_true,
    )
    run_test(
        pipe, "SELECT regexp_substr('the year is unknow', '[0-9]+');", returns_null
    )


def validate_median(res):
    return res == "4.0"


def validate_median_odd(res):
    return res == "4.5"


def validate_percentile1(res):
    return res == "25.0"


def validate_percentile2(res):
    return res == "43.0"


def validate_percentile_disc(res):
    return res == "40.0"


def test_aggregates(pipe):
    extension_path = "./target/debug/liblimbo_percentile.so"
    # assert no function before extension loads
    run_test(
        pipe,
        "SELECT median(1);",
        returns_error_no_func,
        "median agg function returns null when ext not loaded",
    )
    run_test(
        pipe,
        f".load {extension_path}",
        returns_null,
        "load extension command works properly",
    )
    run_test(
        pipe,
        "select median(value) from numbers;",
        validate_median,
        "median agg function works",
    )
    write_to_pipe(pipe, "INSERT INTO numbers (value) VALUES (8.0);\n")
    run_test(
        pipe,
        "select median(value) from numbers;",
        validate_median_odd,
        "median agg function works with odd number of elements",
    )
    run_test(
        pipe,
        "SELECT percentile(value, percent) from test;",
        validate_percentile1,
        "test aggregate percentile function with 2 arguments works",
    )
    run_test(
        pipe,
        "SELECT percentile(value, 55) from test;",
        validate_percentile2,
        "test aggregate percentile function with 1 argument works",
    )
    run_test(
        pipe, "SELECT percentile_cont(value, 0.25) from test;", validate_percentile1
    )
    run_test(
        pipe, "SELECT percentile_disc(value, 0.55) from test;", validate_percentile_disc
    )


# Encoders and decoders
def validate_url_encode(a):
    return a == "%2Fhello%3Ftext%3D%28%E0%B2%A0_%E0%B2%A0%29"


def validate_url_decode(a):
    return a == "/hello?text=(ಠ_ಠ)"


def validate_hex_encode(a):
    return a == "68656c6c6f"


def validate_hex_decode(a):
    return a == "hello"


def validate_base85_encode(a):
    return a == "BOu!rDZ"


def validate_base85_decode(a):
    return a == "hello"


def validate_base32_encode(a):
    return a == "NBSWY3DP"


def validate_base32_decode(a):
    return a == "hello"


def validate_base64_encode(a):
    return a == "aGVsbG8="


def validate_base64_decode(a):
    return a == "hello"


def test_crypto(pipe):
    extension_path = "./target/debug/liblimbo_crypto.so"
    # assert no function before extension loads
    run_test(
        pipe,
        "SELECT crypto_blake('a');",
        lambda res: "Parse error" in res,
        "crypto_blake3 returns null when ext not loaded",
    )
    run_test(
        pipe,
        f".load {extension_path}",
        returns_null,
        "load extension command works properly",
    )
    # Hashing and Decode
    run_test(
        pipe,
        "SELECT crypto_encode(crypto_blake3('abc'), 'hex');",
        lambda res: res
        == "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
        "blake3 should encrypt correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode(crypto_md5('abc'), 'hex');",
        lambda res: res == "900150983cd24fb0d6963f7d28e17f72",
        "md5 should encrypt correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode(crypto_sha1('abc'), 'hex');",
        lambda res: res == "a9993e364706816aba3e25717850c26c9cd0d89d",
        "sha1 should encrypt correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode(crypto_sha256('abc'), 'hex');",
        lambda a: a
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "sha256 should encrypt correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode(crypto_sha384('abc'), 'hex');",
        lambda a: a
        == "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        "sha384 should encrypt correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode(crypto_sha512('abc'), 'hex');",
        lambda a: a
        == "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
        "sha512 should encrypt correctly",
    )

    # Encoding and Decoding
    run_test(
        pipe,
        "SELECT crypto_encode('hello', 'base32');",
        validate_base32_encode,
        "base32 should encode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_decode('NBSWY3DP', 'base32');",
        validate_base32_decode,
        "base32 should decode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode('hello', 'base64');",
        validate_base64_encode,
        "base64 should encode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_decode('aGVsbG8=', 'base64');",
        validate_base64_decode,
        "base64 should decode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_encode('hello', 'base85');",
        validate_base85_encode,
        "base85 should encode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_decode('BOu!rDZ', 'base85');",
        validate_base85_decode,
        "base85 should decode correctly",
    )

    run_test(
        pipe,
        "SELECT crypto_encode('hello', 'hex');",
        validate_hex_encode,
        "hex should encode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_decode('68656c6c6f', 'hex');",
        validate_hex_decode,
        "hex should decode correctly",
    )

    run_test(
        pipe,
        "SELECT crypto_encode('/hello?text=(ಠ_ಠ)', 'url');",
        validate_url_encode,
        "url should encode correctly",
    )
    run_test(
        pipe,
        "SELECT crypto_decode('%2Fhello%3Ftext%3D%28%E0%B2%A0_%E0%B2%A0%29', 'url');",
        validate_url_decode,
        "url should decode correctly",
    )


def test_series(pipe):
    ext_path = "./target/debug/liblimbo_series"
    run_test(
        pipe,
        "SELECT * FROM generate_series(1, 10);",
        lambda res: "Virtual table generate_series not found" in res,
    )
    run_test(pipe, f".load {ext_path}", returns_null)
    run_test(
        pipe,
        "SELECT * FROM generate_series(1, 10);",
        lambda res: res == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10",
    )
    run_test(
        pipe,
        "SELECT * FROM generate_series(1, 10, 2);",
        lambda res: res == "1\n3\n5\n7\n9",
    )
    run_test(
        pipe,
        "SELECT * FROM generate_series(1, 10, 2, 3);",
        lambda res: "Invalid Argument" in res,
    )
    run_test(
        pipe,
        "SELECT * FROM generate_series(10, 1, -2);",
        lambda res: res == "10\n8\n6\n4\n2",
    )


def main():
    pipe = init_limbo()
    try:
        test_regexp(pipe)
        test_uuid(pipe)
        test_aggregates(pipe)
        test_crypto(pipe)
        test_series(pipe)

    except Exception as e:
        print(f"Test FAILED: {e}")
        pipe.terminate()
        exit(1)
    pipe.terminate()
    print("All tests passed successfully.")


if __name__ == "__main__":
    main()
