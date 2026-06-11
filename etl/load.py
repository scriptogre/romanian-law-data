"""
Stage 3 — load.py (library)

Functions for the "L" stage of ETL:

    write_parquets(parsed_iter)  — stream parsed records into 3 parquet files
    build_fts_index()             — build the BM25 DuckDB FTS index
    write_combined_sha256()       — emit the manifest hash

Called from `etl.transform` after the cleanup + parse stages. No `main()` —
the merged pipeline runs as `python -m etl.transform`.

Schemas (final v1, no embeddings yet). Column naming follows the rule
`<level>_<role>` so the role of each column is unambiguous when tables are
joined (document_number ≠ article_number ≠ paragraph_number).

    documents.parquet
        id, type, document_number, document_citation, issuer, title, content,
        adopted_at, published_at, effective_at, gazette_number, status, link,
        synced_at

    articles.parquet
        id, document_id, article_number, article_variant, article_citation, content

    paragraphs.parquet
        id, article_id, paragraph_number, paragraph_citation, content

FTS index build peaks around 3-4 GB of working memory on ~1M articles. We cap
`memory_limit` to 8 GB by default (well under the 16 GB on a public-repo
`ubuntu-latest` runner) and let DuckDB spill to disk via `temp_directory`
if the cap is hit. Override with `FTS_MEMORY_LIMIT` env var.
"""

import hashlib
import os
import shutil
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

import yaml

from etl import LOOKUPS_DIR

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
DOCUMENTS_PATH = DATA_DIR / "documents.parquet"
ARTICLES_PATH = DATA_DIR / "articles.parquet"
PARAGRAPHS_PATH = DATA_DIR / "paragraphs.parquet"
SHA_PATH = DATA_DIR / "laws.sha256"
FTS_DB_PATH = DATA_DIR / "fts.duckdb"
FTS_TEMP_DIR = DATA_DIR / "_fts_temp"
FTS_MEMORY_LIMIT = os.environ.get("FTS_MEMORY_LIMIT", "8GB")


# Shorthand for the most common act types. Unmapped types fall back to title-case.
TYPE_SHORTHAND: dict[str, str] = yaml.safe_load(
    (LOOKUPS_DIR / "type_shorthand.yaml").read_text(encoding="utf-8")
)
# Codes are singletons — multiple republicări share the same canonical name.
SINGLETON_CITATIONS: dict[str, str] = yaml.safe_load(
    (LOOKUPS_DIR / "singleton_citations.yaml").read_text(encoding="utf-8")
)


DOCUMENTS_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("type", pa.string()),
        ("document_number", pa.string()),
        ("document_citation", pa.string()),
        ("issuer", pa.string()),
        ("title", pa.string()),
        ("content", pa.string()),
        ("adopted_at", pa.date32()),
        ("published_at", pa.date32()),
        ("effective_at", pa.date32()),
        ("gazette_number", pa.int64()),
        ("status", pa.string()),
        ("link", pa.string()),
        ("synced_at", pa.timestamp("us")),
    ]
)
ARTICLES_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("document_id", pa.int64()),
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

BATCH_DOCUMENTS = 2_000
BATCH_ARTICLES = 10_000
BATCH_PARAGRAPHS = 50_000


def _parse_date(value):
    """Accept either a date already (Polars row) or an ISO string (legacy)."""
    if value is None:
        return None
    if hasattr(value, "year"):  # already datetime.date / date32
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _strip_bom(value: str | None) -> str | None:
    """Defensive BOM strip on text fields."""
    if value is None:
        return None
    cleaned = value.lstrip("﻿").strip()
    return cleaned or None


def build_document_citation(type_, document_number, adopted_at, issuer) -> str:
    """Build the act-level citation a Romanian lawyer would type.

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
    if document_number and year:
        return (
            f"{short} {document_number}"
            if "/" in document_number
            else f"{short} {document_number}/{year}"
        )
    if document_number:
        return f"{short} {document_number}"
    if adopted_at:
        return f"{short} din {adopted_at.isoformat()}"
    return short


def _flush(writer: pq.ParquetWriter, rows: list[dict], schema: pa.Schema) -> None:
    if not rows:
        return
    writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    rows.clear()


def write_parquets(parsed_iter: Iterable[dict]) -> tuple[int, int, int]:
    """Stream parsed records into 3 parquet files. Returns (n_documents, n_articles, n_paragraphs).

    `parsed_iter` is any iterable of dicts shaped:
        {
            "raw": { Titlu, Text, TipAct, Numar, Emitent, ..., AdoptedAt, ... },
            "articles": [ {number, number_variant, full_path, content, paragraphs: [...]}, ... ]
        }

    Memory is O(batch_size), not O(corpus). At 186k acts the previous
    list-then-DataFrame approach OOM'd.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    synced_at = datetime.now(UTC).replace(tzinfo=None)

    next_document_id = next_article_id = next_paragraph_id = 1
    n_documents = n_articles = n_paragraphs = 0
    document_buf: list[dict] = []
    article_buf: list[dict] = []
    paragraph_buf: list[dict] = []

    with (
        pq.ParquetWriter(DOCUMENTS_PATH, DOCUMENTS_SCHEMA, compression="zstd") as documents_writer,
        pq.ParquetWriter(ARTICLES_PATH, ARTICLES_SCHEMA, compression="zstd") as articles_writer,
        pq.ParquetWriter(
            PARAGRAPHS_PATH, PARAGRAPHS_SCHEMA, compression="zstd"
        ) as paragraphs_writer,
    ):
        for parsed in parsed_iter:
            raw = parsed["raw"]

            document_id = next_document_id
            next_document_id += 1

            type_ = raw.get("TipAct")
            document_number = raw.get("Numar")
            issuer = raw.get("Emitent")
            adopted_at = _parse_date(raw.get("AdoptedAt"))

            document_buf.append(
                {
                    "id": document_id,
                    "type": type_,
                    "document_number": document_number,
                    "document_citation": build_document_citation(
                        type_, document_number, adopted_at, issuer
                    ),
                    "issuer": issuer,
                    "title": _strip_bom(raw.get("Titlu")),
                    "content": _strip_bom(raw.get("Text")) or "",
                    "adopted_at": adopted_at,
                    "published_at": _parse_date(raw.get("PublishedAt")),
                    "effective_at": _parse_date(raw.get("EffectiveAt")),
                    "gazette_number": raw.get("GazetteNumber"),
                    "status": raw.get("Status"),
                    "link": raw.get("LinkHtml"),
                    "synced_at": synced_at,
                }
            )
            n_documents += 1

            # transform emits internal article keys (`number`, `number_variant`,
            # `full_path`); map them to the level-prefixed schema names.
            for article in parsed["articles"]:
                article_id = next_article_id
                next_article_id += 1
                article_buf.append(
                    {
                        "id": article_id,
                        "document_id": document_id,
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

            if len(document_buf) >= BATCH_DOCUMENTS:
                _flush(documents_writer, document_buf, DOCUMENTS_SCHEMA)
            if len(article_buf) >= BATCH_ARTICLES:
                _flush(articles_writer, article_buf, ARTICLES_SCHEMA)
            if len(paragraph_buf) >= BATCH_PARAGRAPHS:
                _flush(paragraphs_writer, paragraph_buf, PARAGRAPHS_SCHEMA)

            if n_documents % 10_000 == 0:
                logger.info(
                    f"load: progress  documents={n_documents:>7d}  "
                    f"articles={n_articles:>8d}  paragraphs={n_paragraphs:>9d}"
                )

        _flush(documents_writer, document_buf, DOCUMENTS_SCHEMA)
        _flush(articles_writer, article_buf, ARTICLES_SCHEMA)
        _flush(paragraphs_writer, paragraph_buf, PARAGRAPHS_SCHEMA)

    return n_documents, n_articles, n_paragraphs


def build_fts_index() -> None:
    """Build a persistent DuckDB BM25 index over articles.content."""
    logger.info(f"build_fts: start (input={ARTICLES_PATH.name})")
    if not ARTICLES_PATH.exists():
        raise SystemExit(f"missing input: {ARTICLES_PATH}")

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
        n_threads = int(os.environ.get("FTS_THREADS", os.cpu_count() or 4))
        con.execute(f"SET threads={n_threads}")
        con.execute(f"SET temp_directory='{FTS_TEMP_DIR}'")
        con.execute("SET preserve_insertion_order=false")
        logger.info(f"memory_limit={FTS_MEMORY_LIMIT}  threads={n_threads}")

        logger.info(f"materialising articles_fts from {ARTICLES_PATH} ...")
        con.execute(
            f"""
            CREATE TABLE articles_fts AS
            SELECT id, content FROM read_parquet('{ARTICLES_PATH}');
            """
        )
        n = con.execute("SELECT COUNT(*) FROM articles_fts").fetchone()[0]
        logger.info(f"rows: {n:,}")

        # `ignore` defines what is NOT a token separator — we keep Romanian
        # letters + digits; everything else splits. `strip_accents=0` preserves
        # ă/â/î/ș/ț so terms like "bună-credință" tokenize correctly.
        logger.info("building FTS index (Romanian Snowball stemmer)...")
        con.execute(
            r"""
            PRAGMA create_fts_index(
                'articles_fts', 'id', 'content',
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


def write_combined_sha256() -> str:
    """Emit a one-line sha256 manifest covering the 3 parquets."""
    hasher = hashlib.sha256()
    for path in (DOCUMENTS_PATH, ARTICLES_PATH, PARAGRAPHS_PATH):
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
    digest = hasher.hexdigest()
    SHA_PATH.write_text(digest + "\n")
    return digest
