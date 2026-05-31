# Local development loop. Three tiers of feedback speed.
#
#   just test            — unit tests on hand-picked fixtures (<1 sec)
#   just smoke           — full pipeline on 1000 cached raw acts (~3 sec)
#   just local           — full pipeline on entire local cache (~5-10 min)
#   just extract         — one-time API fetch into data/raw_acts.jsonl (~30 min)
#
# Workflow once the cache exists:
#   1. edit transform.py / load.py / lookup yaml
#   2. just test         (locks rule-level behaviour)
#   3. just smoke        (sanity-check end-to-end)
#   4. just local        (full verify before committing)

default:
    @just --list

# Unit tests on fixtures. Subsecond.
test:
    uv run pytest -q

# One-time API fetch. Don't run unless you really need fresh raw data.
extract:
    uv run python -m etl.extract

# Smoke run: head -N data/raw_acts.jsonl through transform | load.
# Set ETL_MAX_ACTS so transform.py knows the cap (not strictly needed for head pipe,
# but useful for log clarity).
smoke n="1000":
    head -{{n}} data/raw_acts.jsonl > /tmp/raw_acts_smoke.jsonl
    cp data/raw_acts.jsonl /tmp/raw_acts_full_backup.jsonl
    mv /tmp/raw_acts_smoke.jsonl data/raw_acts.jsonl
    uv run python -m etl.transform | uv run python -m etl.load
    mv /tmp/raw_acts_full_backup.jsonl data/raw_acts.jsonl

# Full local pipeline run, end to end. Uses the cached raw_acts.jsonl.
local:
    uv run python -m etl.transform | uv run python -m etl.load

# Audit the current parquet bundle via DuckDB.
audit:
    uv run python -c "import duckdb; c = duckdb.connect(); \
        print('acte:    ', c.execute(\"SELECT count(*) FROM read_parquet('data/acte.parquet')\").fetchone()[0]); \
        print('articole:', c.execute(\"SELECT count(*) FROM read_parquet('data/articole.parquet')\").fetchone()[0]); \
        print('alineate:', c.execute(\"SELECT count(*) FROM read_parquet('data/alineate.parquet')\").fetchone()[0])"

# Wipe the local build outputs (keeps raw_acts.jsonl).
clean:
    rm -f data/acte.parquet data/articole.parquet data/alineate.parquet
    rm -f data/fts.duckdb data/laws.sha256 data/parse_report.jsonl
