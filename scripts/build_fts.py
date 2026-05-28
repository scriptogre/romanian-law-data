"""
Stage 5 — build_fts.py

Build a persistent DuckDB file with a BM25 full-text index over
`articole.content`, tokenised with the Snowball Romanian stemmer. Output:
`data/fts.duckdb`.

The app attaches this file read-only at startup, so there is no runtime
index-build cost. Query shape:

    ATTACH 'data/laws/fts.duckdb' AS fts (READ_ONLY);

    SELECT a.full_path, a.content,
           fts.fts_main_articole_fts.match_bm25(af.id, 'omor') AS score
    FROM fts.articole_fts af
    JOIN articole a ON a.id = af.id
    WHERE fts.fts_main_articole_fts.match_bm25(af.id, 'omor') IS NOT NULL
    ORDER BY score DESC LIMIT 20;

The build peaks around 3-4 GB of working memory on ~1M articles. We cap
`memory_limit` to 8 GB by default (well under the 16 GB on a public-repo
`ubuntu-latest` runner) and let DuckDB spill to disk via `temp_directory`
if the cap is hit. Override with `FTS_MEMORY_LIMIT` env var — e.g. set to
'2GB' when running on a memory-constrained laptop, '12GB' on a beefy
machine for the fastest build.
"""

import os
import shutil
from pathlib import Path

import duckdb
from loguru import logger

DATA_DIR = Path(__file__).parent.parent / "data"
ARTICOLE_PATH = DATA_DIR / "articole.parquet"
FTS_DB_PATH = DATA_DIR / "fts.duckdb"
TEMP_DIR = DATA_DIR / "_fts_temp"
MEMORY_LIMIT = os.environ.get("FTS_MEMORY_LIMIT", "8GB")


def main() -> None:
    logger.info(f"build_fts: start (input={ARTICOLE_PATH.name})")
    if not ARTICOLE_PATH.exists():
        raise SystemExit(f"missing input: {ARTICOLE_PATH} — run scripts.export first")

    if FTS_DB_PATH.exists():
        FTS_DB_PATH.unlink()
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(FTS_DB_PATH))
    try:
        con.execute("INSTALL fts")
        con.execute("LOAD fts")
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        con.execute("SET threads=2")
        con.execute(f"SET temp_directory='{TEMP_DIR}'")
        con.execute("SET preserve_insertion_order=false")
        logger.info(f"memory_limit={MEMORY_LIMIT} (override via FTS_MEMORY_LIMIT)")

        logger.info(f"materialising articole_fts from {ARTICOLE_PATH} ...")
        con.execute(
            f"""
            CREATE TABLE articole_fts AS
            SELECT id, content FROM read_parquet('{ARTICOLE_PATH}');
            """
        )
        n = con.execute("SELECT COUNT(*) FROM articole_fts").fetchone()[0]
        logger.info(f"rows: {n:,}")

        # The `ignore` regex defines what is NOT a token separator (i.e. what
        # to strip). We keep Romanian letters + digits; everything else splits.
        # `strip_accents=0` preserves ă/â/î/ș/ț so terms like "bună-credință"
        # tokenize correctly.
        logger.info("building FTS index (Romanian Snowball stemmer)...")
        con.execute(
            r"""
            PRAGMA create_fts_index(
                'articole_fts', 'id', 'content',
                stemmer='romanian',
                stopwords='none',
                ignore='(\.|[^a-zA-ZăâîșțĂÂÎȘȚ0-9])+',
                strip_accents=0,
                lower=1,
                overwrite=1
            );
            """
        )
        con.execute("CHECKPOINT")
    finally:
        con.close()
        shutil.rmtree(TEMP_DIR, ignore_errors=True)

    size_mb = FTS_DB_PATH.stat().st_size / (1024 * 1024)
    logger.success(f"build_fts: DONE — {FTS_DB_PATH} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
