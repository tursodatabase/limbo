#!/bin/bash
# This is a modified version of the clickbench benchmark script from:
# https://github.com/ClickHouse/ClickBench/tree/main/sqlite

REPO_ROOT=$(git rev-parse --show-toplevel)
RELEASE_BUILD_DIR="$REPO_ROOT/target/release"
CLICKBENCH_DIR="$REPO_ROOT/perf/clickbench"

# If sqlite3 doesn't exist, bail
if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "Error: sqlite3 is not installed"
    exit 1
fi

# Build Limbo in release mode if it's not already built
if [ ! -f "$RELEASE_BUILD_DIR/tursodb" ]; then
    echo "Building Limbo..."
    cargo build --bin tursodb --release
fi

# Clean up any existing DB
rm "$CLICKBENCH_DIR/mydb"* || true

# Create DB using Limbo
echo "Creating DB..."
"$RELEASE_BUILD_DIR/tursodb" --quiet "$CLICKBENCH_DIR/mydb" < "$CLICKBENCH_DIR/create.sql"

# Download a subset of the clickbench dataset if it doesn't exist
NUM_ROWS=1000000
if [ ! -f "$CLICKBENCH_DIR/hits.csv" ]; then
    echo "Downloading dataset..."
    if command -v wget >/dev/null 2>&1; then
        wget -O- --no-verbose 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz' | gunzip | head -n $NUM_ROWS > "$CLICKBENCH_DIR/hits.csv"
    elif command -v curl >/dev/null 2>&1; then
        curl -s 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz' | gunzip | head -n $NUM_ROWS > "$CLICKBENCH_DIR/hits.csv"
    else
        echo "Error: Neither wget nor curl is available"
        exit 1
    fi
else
    echo "Using existing hits.csv file"
fi

# Import the dataset into the DB using sqlite (not Limbo, because Limbo doesn't have index insert yet)
echo "Importing dataset..."
sqlite3 "$CLICKBENCH_DIR/mydb" ".import --csv $CLICKBENCH_DIR/hits.csv hits"

# Run the benchmark
"$CLICKBENCH_DIR/run.sh"
