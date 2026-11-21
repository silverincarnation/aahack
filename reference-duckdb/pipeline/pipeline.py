#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline implementing rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

# The fixture runner passes DATA_DIR and OUTPUT_DIR as env vars (and as args).
DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
VERBOSE = os.environ.get("VERBOSE") == "1"

T0 = time.time()


def log(msg):
    if VERBOSE:
        print(f"[{time.time() - T0:6.2f}s] {msg}", file=sys.stderr, flush=True)


if not os.path.isdir(SCRATCH):
    SCRATCH = os.environ.get("TMPDIR", "/tmp")

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)


def iso_ms(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


WS = iso_ms(manifest["window_start"])
WE = iso_ms(manifest["window_end"])
DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def files(sub):
    d = os.path.join(DATA, sub)
    if not os.path.isdir(d):
        return []
    return sorted(os.path.join(d, n) for n in os.listdir(d) if n.endswith(".csv") and os.path.isfile(os.path.join(d, n)))


# ---------------------------------------------------------------------------
# Macros
# ---------------------------------------------------------------------------
# A field is well formed if it has no quote at all, or is exactly "...". The
# quotes are syntax; "" and an empty field are null.
con.execute("""
CREATE MACRO field_ok(x) AS (NOT contains(x, '"')) OR regexp_full_match(x, '"[^"]*"');
CREATE MACRO unq(x) AS CASE WHEN x = '' OR x = '""' THEN NULL
                            WHEN x[1] = '"' THEN substr(x, 2, length(x) - 2)
                            ELSE x END;
CREATE MACRO dim(y, m) AS CASE WHEN m IN (4, 6, 9, 11) THEN 30
                               WHEN m = 2 THEN CASE WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0 THEN 29 ELSE 28 END
                               ELSE 31 END;
CREATE MACRO civil_days(y, m, d) AS (
    ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) // 400) * 146097
    + ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) - ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) // 400) * 400) * 365
    + ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) - ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) // 400) * 400) // 4
    - ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) - ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) // 400) * 400) // 100
    + (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    - 719468
);
-- F1/F2 with all components as integers; NULL unless every part is in range.
CREATE MACRO iso_ms(y, mo, d, h, mi, se, sg, oh, om) AS
    CASE WHEN mo BETWEEN 1 AND 12 AND d BETWEEN 1 AND dim(y, mo) AND h < 24 AND mi < 60 AND se < 60
              AND oh < 24 AND om < 60
         THEN civil_days(y, mo, d)::BIGINT * 86400000 + (h * 3600 + mi * 60 + se)::BIGINT * 1000
              - sg * (oh * 3600 + om * 60)::BIGINT * 1000
    END;
CREATE MACRO ts_ms(s) AS
    CASE
      WHEN s IS NULL THEN NULL
      WHEN regexp_full_match(s, '[0-9]+') THEN
           CASE WHEN length(ltrim(s, '0')) > 15 THEN NULL
                ELSE coalesce(TRY_CAST(nullif(ltrim(s, '0'), '') AS BIGINT), 0) END
      -- strptime checks the calendar (month 13, Feb 30, hour 24, second 60 all fail).
      WHEN length(s) = 20 AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z') THEN
           epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
      WHEN length(s) = 25 AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
           AND substr(s, 21, 2) <= '23' AND substr(s, 24, 2) <= '59' THEN
           epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
           - (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
             * (substr(s, 21, 2)::BIGINT * 3600000 + substr(s, 24, 2)::BIGINT * 60000)
    END;
""")


HAS_CR = None


def has_cr():
    """The generator never writes \r, but the rules allow it. If any input has one,
    use the slow exact reader (read_text, split on \n only); otherwise read_csv."""
    global HAS_CR
    if HAS_CR is None:
        import subprocess
        # grep -q exits 0 at the first \r it finds in any input CSV.
        r = subprocess.run(["grep", "-rqF", "--include=*.csv", "\r", DATA])
        HAS_CR = r.returncode == 0
        log(f"has_cr={HAS_CR}")
    return HAS_CR


FIELD = r'(?:"([^",]*)"|([^",]*))'


def load(table, paths, cols, select="*"):
    """Materialize `table` with the given columns: the unquoted values (NULL for empty)
    of each well-formed data line. Materializing the regex result first stops DuckDB
    from re-evaluating the regex once per column reference."""
    n = len(cols)
    if not paths:
        con.execute(f"CREATE TABLE {table} AS SELECT " + ", ".join(f"NULL::VARCHAR AS {c}" for c in cols) + " WHERE false")
        return
    lst = "[" + ",".join(sql_str(p) for p in paths) + "]"
    if has_cr():
        # Slow exact path: only \n ends a line; drop each file's header line; one regex per line.
        src = f"SELECT unnest(string_split(content, chr(10))[2:]) AS line FROM read_text({lst})"
        pat = "^(" + ",".join([FIELD] * n) + ")$"
        names = ["all"] + [f"g{i}" for i in range(2 * n)]
        con.execute(f"CREATE TEMP TABLE {table}_rx AS SELECT regexp_extract(line, {sql_str(pat)}, {names}) AS r "
                    f"FROM ({src}) WHERE line <> ''")
        vals = ", ".join(f"nullif(r.g{2*i} || r.g{2*i+1}, '') AS {c}" for i, c in enumerate(cols))
        con.execute(f"CREATE TABLE {table} AS SELECT {select} FROM (SELECT {vals} FROM {table}_rx WHERE r.all <> '')")
        con.execute(f"DROP TABLE {table}_rx")
        return
    # Fast path (no \r anywhere): DuckDB splits on every comma with quoting turned off,
    # and drops lines with the wrong field count. A field is well formed if it has no
    # quote or is exactly "..."; then removing the quotes gives the value.
    spec = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in cols) + "}"
    src = (f"read_csv({lst}, columns={spec}, delim=',', quote='', escape='', header=true, auto_detect=false, "
           f"new_line='\\n', strict_mode=true, ignore_errors=true, null_padding=false, all_varchar=true)")
    ok = " AND ".join(f"({c} IS NULL OR NOT contains({c}, '\"') OR regexp_full_match({c}, '\"[^\"]*\"'))" for c in cols)
    vals = ", ".join(f"nullif(replace({c}, '\"', ''), '') AS {c}" for c in cols)
    con.execute(f"CREATE TABLE {table} AS SELECT {select} FROM (SELECT {vals} FROM {src} WHERE {ok})")


# ---------------------------------------------------------------------------
# Links -> person names
# ---------------------------------------------------------------------------
load("links_raw", files("links"), ["a", "b"])
con.execute("CREATE TABLE links AS SELECT a, b FROM links_raw WHERE a IS NOT NULL AND b IS NOT NULL")
con.execute("""
CREATE TABLE link_ids AS
SELECT id, (row_number() OVER () - 1)::BIGINT AS ix FROM (SELECT a AS id FROM links UNION SELECT b FROM links)
""")
n_ids = con.execute("SELECT count(*) FROM link_ids").fetchone()[0]
log(f"links parsed, {n_ids} linked ids")
if n_ids:
    edges = con.execute("""
        SELECT ia.ix AS i, ib.ix AS j FROM links
        JOIN link_ids ia ON ia.id = links.a JOIN link_ids ib ON ib.id = links.b
    """).fetchnumpy()
    i, j = edges["i"].astype(np.int64), edges["j"].astype(np.int64)
    g = coo_matrix((np.ones(len(i), dtype=np.int8), (i, j)), shape=(n_ids, n_ids)).tocsr()
    _, comp = connected_components(g, directed=True, connection="weak")
    comp_tbl = pa.table({"ix": np.arange(n_ids, dtype=np.int64), "comp": comp.astype(np.int64)})
    con.register("comp_tbl", comp_tbl)
    con.execute("""
    CREATE TABLE idmap AS
    WITH c AS (SELECT l.id, t.comp FROM link_ids l JOIN comp_tbl t USING (ix))
    SELECT c.id, n.name FROM c JOIN (SELECT comp, min(id) AS name FROM c GROUP BY comp) n USING (comp)
    """)
else:
    con.execute("CREATE TABLE idmap (id VARCHAR, name VARCHAR)")
log("person names done")

# ---------------------------------------------------------------------------
# Dimension tables (clean files)
# ---------------------------------------------------------------------------
load("products", [os.path.join(DATA, "products.csv")], ["product_id", "category"])
load("campaigns", [os.path.join(DATA, "campaigns.csv")], ["campaign_id", "channel"])

# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
# Parse, and work out everything about each copy that does not depend on dedup.
load("ev_raw", files("events"), ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price"],
     select="""event_id, user_id, ingest_ts, product_id, ts_ms(ts) AS ts,
               event_type IN ('view', 'cart', 'purchase') AS type_ok,
               CASE WHEN event_type = 'purchase' THEN
                    CASE WHEN regexp_full_match(price, '[0-9]+') AND length(ltrim(price, '0')) BETWEEN 1 AND 8
                         THEN TRY_CAST(ltrim(price, '0') AS BIGINT) END
               END AS price, event_type = 'purchase' AS is_purchase""")
log("events parsed")
# Dedup on event_id, keeping the smallest ingest_ts (before validation). ingest_ts is
# always valid F1 (fixed width), so its string order is its time order. Only the few
# duplicated IDs take the expensive path. Then validate the kept copy and drop late events.
con.execute(f"""
CREATE TABLE ev AS
WITH d AS (SELECT event_id FROM ev_raw WHERE event_id IS NOT NULL GROUP BY event_id HAVING count(*) > 1),
k AS (
    SELECT * FROM ev_raw WHERE event_id IS NOT NULL AND event_id NOT IN (SELECT event_id FROM d)
    UNION ALL
    SELECT * FROM (SELECT * FROM ev_raw WHERE event_id IN (SELECT event_id FROM d))
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
)
SELECT coalesce(m.name, e.user_id) AS person, e.ts, e.price, e.product_id
FROM k e LEFT JOIN idmap m ON m.id = e.user_id
WHERE e.user_id IS NOT NULL
  AND e.ts IS NOT NULL AND e.ts >= {WS} AND e.ts < {WE}
  AND e.type_ok AND e.product_id IS NOT NULL
  AND (NOT e.is_purchase OR e.price BETWEEN 1 AND 10000000)
  AND epoch_ms(try_strptime(e.ingest_ts, '%Y-%m-%dT%H:%M:%SZ')) - e.ts <= {DAY}
""")
con.execute("DROP TABLE ev_raw")
log("events cleaned")
load("clicks_t", files("clicks"), ["click_id", "user_id", "campaign_id", "ts"],
     select="click_id, user_id, campaign_id, ts_ms(ts) AS ts")
log("clicks parsed")

# Sessionize: a new session when the gap to the previous event is over 30 min.
con.execute(f"""
CREATE TABLE ev_s AS
SELECT person, ts, price, product_id,
       sum(CASE WHEN prev IS NULL OR ts - prev > {GAP} THEN 1 ELSE 0 END)
           OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sid
FROM (SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS prev FROM ev)
""")
con.execute("DROP TABLE ev")
con.execute(f"""
CREATE TABLE sessions AS
SELECT person, sid, min(ts) AS start, count(*) AS n_events,
       coalesce(sum(price), 0) AS revenue, count(price) > 0 AS purchasing
FROM ev_s GROUP BY person, sid
""")
log("sessions built")

# ---------------------------------------------------------------------------
# Clicks
# ---------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE clicks AS
SELECT DISTINCT coalesce(m.name, c.user_id) AS person, c.click_id, c.campaign_id, c.ts
FROM (
    SELECT * FROM clicks_t
) c
LEFT JOIN idmap m ON m.id = c.user_id
WHERE c.click_id IS NOT NULL AND c.user_id IS NOT NULL
  AND c.ts IS NOT NULL AND c.ts >= {WS - WEEK} AND c.ts < {WE}
  AND c.campaign_id IN (SELECT campaign_id FROM campaigns)
""")
log("clicks cleaned")

# Credit: latest click at or before the start, within 7 days; ties -> smallest click_id.
con.execute("""
CREATE TABLE click_best AS
SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clicks GROUP BY person, ts
""")
con.execute(f"""
CREATE TABLE credited AS
SELECT s.person, s.sid, s.start, s.revenue, s.purchasing,
       CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK} THEN c.campaign_id ELSE 'organic' END AS campaign_id,
       (DATE '1970-01-01' + (s.start // {DAY})::INTEGER) AS day
FROM sessions s ASOF LEFT JOIN click_best c ON s.person = c.person AND s.start >= c.ts
""")
log("credit done")

# Assists: per (person, campaign), merge [t, t + 7d] intervals, then count sessions inside.
con.execute(f"""
CREATE TABLE assist_iv AS
WITH pc AS (SELECT DISTINCT person, campaign_id, ts FROM clicks),
g AS (
    SELECT *, sum(CASE WHEN prev IS NULL OR ts - prev > {WEEK} THEN 1 ELSE 0 END)
                  OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
    FROM (SELECT *, lag(ts) OVER (PARTITION BY person, campaign_id ORDER BY ts) AS prev FROM pc)
)
SELECT person, campaign_id, min(ts) AS lo, max(ts) + {WEEK} AS hi FROM g GROUP BY person, campaign_id, grp
""")
con.execute("""
CREATE TABLE assists AS
SELECT iv.campaign_id, s.day, count(*) AS assisted
FROM credited s JOIN assist_iv iv ON s.person = iv.person AND s.start >= iv.lo AND s.start <= iv.hi
GROUP BY iv.campaign_id, s.day
""")
log("assists done")

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

con.execute(f"""
COPY (
    WITH sd AS (SELECT person, sid, (DATE '1970-01-01' + (start // {DAY})::INTEGER) AS day FROM sessions),
    cat AS (
        SELECT sd.person, sd.day, coalesce(p.category, 'unknown') AS category, sum(e.price) AS amt
        FROM ev_s e JOIN sd USING (person, sid) LEFT JOIN products p ON p.product_id = e.product_id
        WHERE e.price IS NOT NULL GROUP BY ALL
    ),
    top AS (SELECT person, day, arg_min(category, (-amt, category)) AS top_category FROM cat GROUP BY person, day),
    base AS (
        SELECT sd.person, sd.day, count(*) AS sessions, sum(s.n_events) AS events, sum(s.revenue) AS revenue_cents
        FROM sessions s JOIN sd USING (person, sid) GROUP BY sd.person, sd.day
    )
    SELECT b.person AS user_id, b.day, b.sessions::BIGINT AS sessions, b.events::BIGINT AS events,
           b.revenue_cents::BIGINT AS revenue_cents, t.top_category
    FROM base b LEFT JOIN top t USING (person, day)
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("user_daily written")

con.execute(f"""
COPY (
    WITH cr AS (
        SELECT campaign_id, day, count(*) AS sessions, count(*) FILTER (WHERE purchasing) AS purchasing_sessions,
               sum(revenue) AS revenue_cents
        FROM credited GROUP BY campaign_id, day
    ),
    j AS (
        SELECT coalesce(cr.campaign_id, a.campaign_id) AS campaign_id, coalesce(cr.day, a.day) AS day,
               coalesce(cr.sessions, 0) AS sessions, coalesce(cr.purchasing_sessions, 0) AS purchasing_sessions,
               coalesce(cr.revenue_cents, 0) AS revenue_cents, coalesce(a.assisted, 0) AS assisted_sessions
        FROM cr FULL OUTER JOIN assists a ON cr.campaign_id = a.campaign_id AND cr.day = a.day
    )
    SELECT j.campaign_id, CASE WHEN j.campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
           j.day, j.sessions::BIGINT AS sessions, j.purchasing_sessions::BIGINT AS purchasing_sessions,
           j.revenue_cents::BIGINT AS revenue_cents, j.assisted_sessions::BIGINT AS assisted_sessions
    FROM j LEFT JOIN campaigns c ON c.campaign_id = j.campaign_id AND j.campaign_id <> 'organic'
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("campaign_daily written")
