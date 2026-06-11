"""
Stage 2+3 — transform.py

Orchestrator for everything after extract. Reads `data/raw_documents/*.parquet`,
runs the Polars cleanup pipeline, parses each cleaned act into articles +
paragraphs, writes the three parquets via load.py, validates them with
Pandera, and builds the FTS index.

Single process. No JSON pipe between stages — parsed records are passed
to `load.write_parquets` as a Python generator, eliminating the ~10 GB
encode/decode the two-process design needed.

Pipeline:

    scan_parquet(raw_documents/*.parquet)
        ↓
    cleanup (Polars LazyFrame: cedilla, BOM, whitespace, issuer recovery,
             3 date extractions, dedup) — runs on all cores via Rust/Rayon
        ↓
    collect(engine="streaming") — chunked execution, memory-bounded
        ↓
    iter_rows + parse (Python regex; lookaround forces per-row execution)
        ↓
    write_parquets (PyArrow streaming writers)
        ↓
    Pandera schema validation
        ↓
    build_fts_index (DuckDB BM25)
"""

import json
import os
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from itertools import chain
from pathlib import Path

import polars as pl
from loguru import logger

from etl.load import build_fts_index, write_combined_sha256, write_parquets
from etl.schemas import validate_parquets, validate_relationships
from etl.transforms import dates, dedup, issuers, relationships, text
from etl.transforms.parse import extract_articles, extract_paragraphs
from etl.transforms.quality import (
    HIGH_QUALITY,
    MEDIUM_QUALITY,
    compute_quality,
)

REPO_ROOT = Path(__file__).parent.parent
INPUT_DIR = REPO_ROOT / "data" / "raw_documents"
REPORT_PATH = REPO_ROOT / "data" / "parse_report.jsonl"

MIN_ARTICLES_FOR_CLEAN_PARSE = 1
PARSE_WORKERS = int(os.environ.get("ETL_TRANSFORM_WORKERS", max(1, (os.cpu_count() or 4))))
PARSE_CHUNKSIZE = 100
# Acts fed to the pool per slice. pool.map pickles its whole input up front, so
# mapping all ~250k rows at once duplicates the corpus in the task queue and OOMs
# the 16 GB runner. Slicing caps in-flight tasks (+ buffered results) to this.
PARSE_BATCH = int(os.environ.get("ETL_TRANSFORM_BATCH", 5_000))


def cleanup(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Apply every cleanup transform in order. The composition is the contract."""
    return (
        lf.pipe(text.normalize_nfc)
        .pipe(text.decode_html_entities)
        .pipe(text.fix_replacement_chars)
        .pipe(text.fix_cedilla)
        .pipe(text.strip_bom)
        .pipe(text.clean_titlu)
        .pipe(text.clean_text)
        .pipe(text.normalize_numar)
        .pipe(text.blank_titlu_to_null)
        .pipe(issuers.recover_emitent)
        .pipe(issuers.apply_aliases)
        .pipe(dates.extract_adopted)
        .pipe(dates.extract_gazette)
        .pipe(dates.extract_effective)
        .pipe(dates.clamp_far_future)
        .pipe(dedup.by_titlu_emitent)
        # Status stays NULL: planned to derive in a view from relationships edges.
        .with_columns(pl.lit(None, dtype=pl.Utf8).alias("Status"))
    )


def parse_document(cleaned: dict) -> dict:
    """Split one cleaned act into articles + paragraphs + quality.

    `cleaned` is a row from the cleanup LazyFrame — keys mirror the
    normalized field names (Titlu, Text, TipAct, ..., AdoptedAt, PublishedAt,
    EffectiveAt, GazetteNumber).
    """
    text_body = cleaned.get("Text") or ""
    articles = extract_articles(text_body)
    is_fallback = len(articles) < MIN_ARTICLES_FOR_CLEAN_PARSE

    if is_fallback:
        articles = [
            {
                "number": None,
                "number_variant": None,
                "full_path": "(unparsed)",
                "content": text_body.strip(),
            }
        ]

    for article in articles:
        article["paragraphs"] = extract_paragraphs(article["full_path"], article["content"])

    quality = compute_quality(
        text_body, articles if not is_fallback else [], is_fallback=is_fallback
    )
    return {"raw": cleaned, "articles": articles, "quality": quality}


def transform_document(raw_document: dict) -> dict:
    """End-to-end transform on a single raw SOAP dict. Used by tests."""
    lf = pl.LazyFrame([raw_document])
    cleaned_rows = cleanup(lf).collect().to_dicts()
    if not cleaned_rows:
        return {"raw": raw_document, "articles": [], "quality": None}
    return parse_document(cleaned_rows[0])


def _parsed_records(
    parsed_iter: Iterator[dict],
    report_fp,
    bands: Counter,
    gates: Counter,
    scoresum: list[float],
) -> Iterator[dict]:
    """Tap the parsed iterator: write the per-act report and track band counts."""
    for i, parsed in enumerate(parsed_iter, start=1):
        if i % 10_000 == 0:
            avg = scoresum[0] / i
            logger.info(
                f"parse: progress documents={i:>7d}  "
                f"high={bands['high']:>5d}  med={bands['medium']:>4d}  "
                f"low={bands['low']:>4d}  fallback={bands['intentional-fallback']:>5d}  "
                f"mean_score={avg:.3f}"
            )

        quality = parsed["quality"]
        raw = parsed["raw"]
        bands[quality["band"]] += 1
        scoresum[0] += quality["score"]
        if quality.get("gate"):
            gates[quality["gate"]] += 1

        report_fp.write(
            json.dumps(
                {
                    "title": raw.get("Titlu"),
                    "type": raw.get("TipAct"),
                    "number": raw.get("Numar"),
                    "text_length": len(raw.get("Text") or ""),
                    "score": quality["score"],
                    "band": quality["band"],
                    "gate": quality.get("gate"),
                    "signals": quality["signals"],
                    "expected_markers": quality["expected_markers"],
                    "detected_articles": quality["detected_articles"],
                },
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )
        yield parsed


def main() -> None:
    logger.info(f"pipeline: start (input={INPUT_DIR.name}/)")
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    lf = pl.scan_parquet(INPUT_DIR / "*.parquet")
    df = cleanup(lf).collect(engine="streaming")
    n_unique = df.height
    logger.info(f"cleanup: done. {n_unique} unique documents (post-dedup)")

    bands: Counter[str] = Counter()
    gates: Counter[str] = Counter()
    scoresum = [0.0]
    logger.info(f"parse: starting with {PARSE_WORKERS} workers")
    with (
        ProcessPoolExecutor(max_workers=PARSE_WORKERS) as pool,
        REPORT_PATH.open("w", encoding="utf-8") as report,
    ):
        # One pool.map per slice, chained lazily: chain.from_iterable pulls the
        # next slice's map only once the current slice drains, so the pool never
        # holds more than PARSE_BATCH rows of pickled tasks at a time.
        parsed_iter = chain.from_iterable(
            pool.map(parse_document, batch.iter_rows(named=True), chunksize=PARSE_CHUNKSIZE)
            for batch in df.iter_slices(PARSE_BATCH)
        )
        n_documents, n_articles, n_paragraphs = write_parquets(
            _parsed_records(parsed_iter, report, bands, gates, scoresum)
        )

    # The cleaned DataFrame holds full body text for ~250k acts (3-5 GB
    # uncompressed). Release before Pandera reloads the documents parquet, which
    # otherwise OOMs the 16 GB CI runner.
    del df

    avg = scoresum[0] / n_unique if n_unique else 0.0
    logger.info(f"  documents: {n_documents:>8d} rows")
    logger.info(f"  articles:  {n_articles:>8d} rows")
    logger.info(f"  paragraphs:{n_paragraphs:>8d} rows")
    logger.info(f"  mean quality:           {avg:.3f}")
    logger.info(f"  high   (≥{HIGH_QUALITY}):           {bands['high']:>6d}")
    logger.info(f"  medium ({MEDIUM_QUALITY}–{HIGH_QUALITY}):       {bands['medium']:>6d}")
    logger.info(f"  low    (<{MEDIUM_QUALITY}):           {bands['low']:>6d}")
    logger.info(f"  intentional fallback:  {bands['intentional-fallback']:>6d}")
    if gates:
        logger.warning(
            f"  gate-downgraded:       low={gates['detection_recall_low']:>4d}  "
            f"med={gates['detection_recall_medium']:>4d}  "
            f"(see parse_report `gate` field)"
        )

    # 3. Validate written parquets against Pandera schemas.
    validate_parquets(
        REPO_ROOT / "data" / "documents.parquet",
        REPO_ROOT / "data" / "articles.parquet",
        REPO_ROOT / "data" / "paragraphs.parquet",
    )

    # 4. Build FTS index over articles.
    build_fts_index()

    # 5. Relationship edges from the cached affects_html (no FK; targets can be
    #    outside the corpus). Skipped if the relationships cache isn't present.
    n_edges = relationships.build_relationships()
    if n_edges:
        validate_relationships(relationships.OUT_PATH)

    # 6. SHA256 manifest of the 3 parquets.
    digest = write_combined_sha256()
    logger.info(f"  sha256: {digest}")
    logger.success(
        f"pipeline: DONE — {n_documents} documents / {n_articles} articles / {n_paragraphs} paragraphs "
        f"/ {n_edges} relationships + FTS"
    )


if __name__ == "__main__":
    main()
