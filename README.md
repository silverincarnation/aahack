# Shopping Sessions: Rust solution

One native binary (`src/main.rs`) implementing every rule in `rules.md`. The DuckDB version in the sibling folder is the readable reference; the two agree row for row on the fixtures, the dev answer and 8 generated seeds.

Checked:
- 36/36 fixtures (`python3 scripts/run_fixtures.py fixtures -- target/release/ss`, or `make fixtures` with Docker).
- Exact match with `dev-answer.zip` on the seed-1 1M-event dev set.
- Inside the scorer's docker flags with only 4 CPUs: 20M events 5.9 s, 60M events 21 s (DuckDB version: about 25 s and 85 s).

Pipeline:
1. mmap every CSV and cut files into line-aligned 4 MB chunks (header line skipped); a hand-written parser implements the dialect (only `\n` ends a line, exact field count, quotes only around a whole field).
2. Linked IDs: IDs sharded by hash, union-find, each person named by the byte-wise smallest ID; a flat open-addressing table maps any linked ID to its person.
3. Events, pass 1: hash every event_id to find the duplicated ones. Pass 2: parse and validate each copy; unique IDs go straight to per-person buckets, copies of duplicated IDs are sorted and the smallest `ingest_ts` copy wins (then must be valid).
4. Clicks: validate, bucket by person.
5. Per person bucket (parallel): sort events and clicks by time, cut sessions (gap over 30 min), credit with a pointer to the latest click at or before the start (ties: smallest click_id), assists with a sliding `[start - 7d, start]` window of campaign counts.
6. Parquet written in parallel parts, uncompressed.

Set `VERBOSE=1` for per-step timings on stderr; `NCPU` sets the thread count.
