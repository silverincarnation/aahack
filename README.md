# Shopping Sessions: Rust solution

One native binary implementing every rule in `rules.md`.

- `src/main.rs`: the fast path. Every ID is packed into a u128 (bytes big-endian, length in the low byte), so IDs compare exactly, and in byte order, as integers. An AVX2 scanner finds commas, quotes and newlines 64 bytes at a time. Events are scattered by event_id for dedup, survivors and clicks by person, and each person partition is sorted and sessionized.
- `src/general.rs`: the general implementation (string keys). The fast path hands over to it, before writing anything, if the data breaks one of its assumptions (an ID over 15 bytes, more than 127 categories, a window over 40 days).
- `reference-duckdb/`: the DuckDB version, used only for diff tests.

Checked: 36/36 fixtures, exact match with `dev-answer.zip`, and row-for-row equal to the DuckDB version on 14 generated seeds.

Run locally: `cargo build --release && python3 scripts/run_fixtures.py fixtures -- target/release/ss`. `VERBOSE=1` prints per-step timings, `FORCE_GENERAL=1` runs the general implementation.
