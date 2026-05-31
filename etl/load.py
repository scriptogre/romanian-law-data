"""
Stage 3 — load.py

Read parsed acts from stdin (one JSONL line per act, as emitted by
transform.py), stream them into three parquet files, then build a persistent
DuckDB BM25 index over `articole.content`. The pipe-only input avoids
materializing the ~10 GB parsed.jsonl on disk; run as:

    python -m etl.transform | python -m etl.load

Writes happen in batches via pyarrow.parquet.ParquetWriter so memory stays
O(batch_size) instead of O(full corpus). At 186k acts the previous
list-then-DataFrame approach OOM'd.

Schemas (final v1, no embeddings yet). Column naming follows the rule
`<level>_<role>` so the role of each column is unambiguous when tables are
joined (act_number ≠ article_number ≠ paragraph_number).

    acte.parquet
        id              INT64
        type            STRING       — LEGE, OUG, HG, ORDIN, DECIZIE, ...
        act_number      STRING NULL  — raw act number from SOAP: "287", "75", or NULL
        act_citation    STRING       — display label: "Legea 287/2009", "OUG 100/2024", "Codul Civil"
        issuer          STRING       — emitter (uppercase)
        title           STRING       — full official title
        content         STRING       — full raw act text
        adopted_at      DATE   NULL
        published_at    DATE   NULL  — gazette publication
        effective_at    DATE   NULL  — entry into force
        gazette_number  INT64  NULL
        link            STRING NULL  — legislatie.just.ro URL
        synced_at       TIMESTAMP

    articole.parquet
        id                INT64
        act_id            INT64        — FK → acte.id
        article_number    INT64  NULL  — ordinal within the act: 188
        article_variant   STRING NULL  — suffix when present: "bis", "ter", "^1" ...
        article_citation  STRING       — display label: "Art. 188", "Art. 188 bis"
        content           STRING       — article text (all paragraphs concatenated)

    alineate.parquet
        id                  INT64
        article_id          INT64        — FK → articole.id
        paragraph_number    INT64  NULL  — ordinal within the article (NULL = monolithic article)
        paragraph_citation  STRING       — display label: "Art. 188 alin. (1)"
        content             STRING       — paragraph text

FTS index build peaks around 3-4 GB of working memory on ~1M articles. We cap
`memory_limit` to 8 GB by default (well under the 16 GB on a public-repo
`ubuntu-latest` runner) and let DuckDB spill to disk via `temp_directory`
if the cap is hit. Override with `FTS_MEMORY_LIMIT` env var.
"""

import hashlib
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from loguru import logger

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
LOOKUPS_DIR = DATA_DIR / "lookups"
ACTE_PATH = DATA_DIR / "acte.parquet"
ARTICOLE_PATH = DATA_DIR / "articole.parquet"
ALINEATE_PATH = DATA_DIR / "alineate.parquet"
SHA_PATH = DATA_DIR / "laws.sha256"
FTS_DB_PATH = DATA_DIR / "fts.duckdb"
FTS_TEMP_DIR = DATA_DIR / "_fts_temp"
FTS_MEMORY_LIMIT = os.environ.get("FTS_MEMORY_LIMIT", "8GB")


def _load_yaml(name: str) -> dict:
    with (LOOKUPS_DIR / f"{name}.yaml").open(encoding="utf-8") as f:
        return yaml.safe_load(f)


# Shorthand for the most common act types — produces the form Romanian
# lawyers actually use in citations (e.g. "Legea 287/2009" not "LEGE 287").
# Unmapped types fall back to title-case of the raw type.
TYPE_SHORTHAND: dict[str, str] = _load_yaml("type_shorthand")

# Codes are singletons — multiple republicări share the same canonical name.
SINGLETON_CITATIONS: dict[str, str] = _load_yaml("singleton_citations")


ACTS_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("type", pa.string()),
        ("act_number", pa.string()),
        ("act_citation", pa.string()),
        ("issuer", pa.string()),
        ("title", pa.string()),
        ("content", pa.string()),
        ("adopted_at", pa.date32()),
        ("published_at", pa.date32()),
        ("effective_at", pa.date32()),
        ("gazette_number", pa.int64()),
        ("link", pa.string()),
        ("synced_at", pa.timestamp("us")),
    ]
)
ARTICLES_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("act_id", pa.int64()),
        ("article_number", pa.int64()),
        ("article_variant", pa.string()),
        ("article_citation", pa.string()),
        ("content", pa.string()),
    ]
)
PARAGRAPHS_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("article_id", pa.int64()),
        ("paragraph_number", pa.int64()),
        ("paragraph_citation", pa.string()),
        ("content", pa.string()),
    ]
)

BATCH_ACTS = 2_000
BATCH_ARTICLES = 10_000
BATCH_PARAGRAPHS = 50_000


def _parse_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _strip_bom(value: str | None) -> str | None:
    """Defensive BOM strip on text fields read from the parse stream."""
    if value is None:
        return None
    cleaned = value.lstrip("﻿").strip()
    return cleaned or None


def build_act_citation(
    type_: str | None,
    act_number: str | None,
    adopted_at,
    issuer: str | None,
) -> str:
    """Build the act-level citation a Romanian lawyer would type. Always returns a string.

    Year comes from `adopted_at` only — published_at (M.Of. date) can shift across
    years for acts adopted late December, so using it as a fallback would silently
    misattribute ~5 acts/year.
    """
    if not type_:
        return ""

    if type_ in SINGLETON_CITATIONS:
        return SINGLETON_CITATIONS[type_]

    short = TYPE_SHORTHAND.get(type_, type_.title())
    # "HG" is only correct for government-issued hotărâri. CCR/ÎCCJ hotărâri
    # are a different thing — fall back to "Hotărârea".
    if type_ == "HOTĂRÂRE" and issuer and not issuer.startswith("GUVERNUL"):
        short = "Hotărârea"

    year = adopted_at.year if adopted_at else None
    if act_number and year:
        return f"{short} {act_number}" if "/" in act_number else f"{short} {act_number}/{year}"
    if act_number:
        return f"{short} {act_number}"
    if adopted_at:
        return f"{short} din {adopted_at.isoformat()}"
    return short


def _flush(writer: pq.ParquetWriter, rows: list[dict], schema: pa.Schema) -> None:
    if not rows:
        return
    writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    rows.clear()


def write_parquets() -> tuple[int, int, int]:
    """Stream parsed acts from stdin → three parquet files. Returns row counts."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    synced_at = datetime.now(UTC).replace(tzinfo=None)

    next_act_id = 1
    next_article_id = 1
    next_paragraph_id = 1

    act_buf: list[dict] = []
    article_buf: list[dict] = []
    paragraph_buf: list[dict] = []

    n_acts = n_articles = n_paragraphs = 0

    with (
        pq.ParquetWriter(ACTE_PATH, ACTS_SCHEMA, compression="zstd") as acts_writer,
        pq.ParquetWriter(ARTICOLE_PATH, ARTICLES_SCHEMA, compression="zstd") as articles_writer,
        pq.ParquetWriter(ALINEATE_PATH, PARAGRAPHS_SCHEMA, compression="zstd") as paragraphs_writer,
    ):
        for line in sys.stdin:
            parsed = json.loads(line)
            raw = parsed["raw"]

            act_id = next_act_id
            next_act_id += 1

            type_ = raw.get("TipAct")
            act_number = raw.get("Numar")
            issuer = raw.get("Emitent")
            adopted_at = _parse_date(raw.get("AdoptedAt"))

            act_buf.append(
                {
                    "id": act_id,
                    "type": type_,
                    "act_number": act_number,
                    "act_citation": build_act_citation(type_, act_number, adopted_at, issuer),
                    "issuer": issuer,
                    "title": _strip_bom(raw.get("Titlu")),
                    "content": _strip_bom(raw.get("Text")) or "",
                    "adopted_at": adopted_at,
                    "published_at": _parse_date(raw.get("PublishedAt")),
                    "effective_at": _parse_date(raw.get("EffectiveAt")),
                    "gazette_number": raw.get("GazetteNumber"),
                    "link": raw.get("LinkHtml"),
                    "synced_at": synced_at,
                }
            )
            n_acts += 1

            # transform.py emits dicts with internal keys (`number`, `number_variant`,
            # `full_path`); we map them here to the level-prefixed schema names.
            for article in parsed["articles"]:
                article_id = next_article_id
                next_article_id += 1
                article_buf.append(
                    {
                        "id": article_id,
                        "act_id": act_id,
                        "article_number": article["number"],
                        "article_variant": article["number_variant"],
                        "article_citation": article["full_path"],
                        "content": article["content"],
                    }
                )
                n_articles += 1

                for paragraph in article["paragraphs"]:
                    paragraph_buf.append(
                        {
                            "id": next_paragraph_id,
                            "article_id": article_id,
                            "paragraph_number": paragraph["number"],
                            "paragraph_citation": paragraph["full_path"],
                            "content": paragraph["content"],
                        }
                    )
                    next_paragraph_id += 1
                    n_paragraphs += 1

            if len(act_buf) >= BATCH_ACTS:
                _flush(acts_writer, act_buf, ACTS_SCHEMA)
            if len(article_buf) >= BATCH_ARTICLES:
                _flush(articles_writer, article_buf, ARTICLES_SCHEMA)
            if len(paragraph_buf) >= BATCH_PARAGRAPHS:
                _flush(paragraphs_writer, paragraph_buf, PARAGRAPHS_SCHEMA)

            if n_acts % 10_000 == 0:
                logger.info(
                    f"load: progress  acte={n_acts:>7d}  "
                    f"articole={n_articles:>8d}  alineate={n_paragraphs:>9d}"
                )

        _flush(acts_writer, act_buf, ACTS_SCHEMA)
        _flush(articles_writer, article_buf, ARTICLES_SCHEMA)
        _flush(paragraphs_writer, paragraph_buf, PARAGRAPHS_SCHEMA)

    return n_acts, n_articles, n_paragraphs


def build_fts_index() -> None:
    """Build a persistent DuckDB BM25 index over articole.content."""
    logger.info(f"build_fts: start (input={ARTICOLE_PATH.name})")
    if not ARTICOLE_PATH.exists():
        raise SystemExit(f"missing input: {ARTICOLE_PATH}")

    if FTS_DB_PATH.exists():
        FTS_DB_PATH.unlink()
    if FTS_TEMP_DIR.exists():
        shutil.rmtree(FTS_TEMP_DIR)
    FTS_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(FTS_DB_PATH))
    try:
        con.execute("INSTALL fts")
        con.execute("LOAD fts")
        con.execute(f"SET memory_limit='{FTS_MEMORY_LIMIT}'")
        con.execute("SET threads=2")
        con.execute(f"SET temp_directory='{FTS_TEMP_DIR}'")
        con.execute("SET preserve_insertion_order=false")
        logger.info(f"memory_limit={FTS_MEMORY_LIMIT} (override via FTS_MEMORY_LIMIT)")

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
        shutil.rmtree(FTS_TEMP_DIR, ignore_errors=True)

    size_mb = FTS_DB_PATH.stat().st_size / (1024 * 1024)
    logger.success(f"build_fts: DONE — {FTS_DB_PATH} ({size_mb:.1f} MB)")


def _write_combined_sha256() -> str:
    hasher = hashlib.sha256()
    for path in (ACTE_PATH, ARTICOLE_PATH, ALINEATE_PATH):
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
    digest = hasher.hexdigest()
    SHA_PATH.write_text(digest + "\n")
    return digest


def main() -> None:
    logger.info("load: start (input=stdin)")
    n_acts, n_articles, n_paragraphs = write_parquets()
    digest = _write_combined_sha256()

    logger.info(f"  acte:     {n_acts:>8d} rows  → {ACTE_PATH}")
    logger.info(f"  articole: {n_articles:>8d} rows  → {ARTICOLE_PATH}")
    logger.info(f"  alineate: {n_paragraphs:>8d} rows  → {ALINEATE_PATH}")
    logger.info(f"  sha256:   {digest}")

    build_fts_index()

    logger.success(
        f"load: DONE — {n_acts} acte / {n_articles} articole / {n_paragraphs} alineate + FTS"
    )


if __name__ == "__main__":
    main()
