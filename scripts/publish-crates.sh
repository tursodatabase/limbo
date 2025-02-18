#!/bin/sh

cargo publish --dry-run -p limbo_macros
cargo publish --dry-run -p limbo_ext
cargo publish --dry-run -p limbo_crypto
cargo publish --dry-run -p limbo_percentile
cargo publish --dry-run -p limbo_regexp
cargo publish --dry-run -p limbo_series
cargo publish --dry-run -p limbo_time
cargo publish --dry-run -p limbo_uuid
cargo publish --dry-run -p limbo_core
cargo publish --dry-run -p limbo
