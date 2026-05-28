"""
Stage 4 — export.py

Read parsed acts from stdin (one JSONL line per act, as emitted by parse.py)
and stream them into three parquet files plus a sha256 digest. The pipe-only
input avoids materializing the ~10 GB parsed.jsonl on disk; run as:

    python -m scripts.parse | python -m scripts.export

Writes happen in batches via pyarrow.parquet.ParquetWriter so memory stays
O(batch_size) instead of O(full corpus). At 186k acts the previous
list-then-DataFrame approach OOM'd.

Schemas (final v1, no embeddings yet):

    acte.parquet
        id                  INT64
        type                STRING       — LEGE, OUG, HG, ORDIN, DECIZIE, ...
        number              STRING NULL  — "287", "75", or NULL (raw)
        canonical_citation  STRING       — "Legea 287/2009", "OUG 100/2024", "Codul Civil"
        issuer              STRING       — emitter (uppercase)
        title               STRING
        content             STRING       — full raw act text
        adopted_at          DATE   NULL
        published_at        DATE   NULL  — gazette publication
        effective_at        DATE   NULL  — entry into force
        gazette_number      INT64  NULL
        link                STRING NULL
        synced_at           TIMESTAMP

    articole.parquet
        id              INT64
        act_id          INT64
        number          INT64  NULL
        number_variant  STRING NULL  — "bis", "ter", "^1" ...
        full_path       STRING       — "Art. 188 bis"
        content         STRING

    alineate.parquet
        id              INT64
        article_id      INT64
        number          INT64  NULL  — NULL = whole article, no alineat
        full_path       STRING       — "Art. 188 alin. (1)"
        content         STRING
"""

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

DATA_DIR = Path(__file__).parent.parent / "data"
ACTE_PATH = DATA_DIR / "acte.parquet"
ARTICOLE_PATH = DATA_DIR / "articole.parquet"
ALINEATE_PATH = DATA_DIR / "alineate.parquet"
SHA_PATH = DATA_DIR / "laws.sha256"

ACTS_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("type", pa.string()),
        ("number", pa.string()),
        ("canonical_citation", pa.string()),
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
        ("number", pa.int64()),
        ("number_variant", pa.string()),
        ("full_path", pa.string()),
        ("content", pa.string()),
    ]
)
PARAGRAPHS_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("article_id", pa.int64()),
        ("number", pa.int64()),
        ("full_path", pa.string()),
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


# Shorthand mapping for the most common act types — produces the form Romanian
# lawyers actually use in citations (e.g. "Legea 287/2009" not "LEGE 287").
# Unmapped types fall back to title-case of the raw type.
_TYPE_SHORTHAND: dict[str, str] = {
    "LEGE": "Legea",
    "ORDONANȚĂ DE URGENȚĂ": "OUG",
    "ORDONANȚĂ": "OG",
    "HOTĂRÂRE": "HG",  # overridden below for non-GUVERNUL issuers
    "ORDIN": "Ordinul",
    "DECIZIE": "Decizia",
    "DECRET": "Decretul",
    "DECRET-LEGE": "Decretul-lege",
    "REGULAMENT": "Regulamentul",
    "RECTIFICARE": "Rectificarea",
    "NORMĂ": "Norma",
    "METODOLOGIE": "Metodologia",
    "COMUNICAT": "Comunicatul",
    "PROCEDURĂ": "Procedura",
    "ANEXĂ": "Anexa",
    "CIRCULARĂ": "Circulara",
    "INSTRUCȚIUNI": "Instrucțiunile",
    "RAPORT": "Raportul",
    "ACORD": "Acordul",
    "PROTOCOL": "Protocolul",
    "AMENDAMENT": "Amendamentul",
    "CONVENȚIE": "Convenția",
    "ÎNCHEIERE": "Încheierea",
    "SENTINȚĂ": "Sentința",
}

# Codes are singletons — multiple republicări share the same canonical name.
_SINGLETON_CITATIONS: dict[str, str] = {
    "CONSTITUȚIE": "Constituția României",
    "CODUL CIVIL": "Codul Civil",
    "CODUL PENAL": "Codul Penal",
    "CODUL DE PROCEDURĂ CIVILĂ": "Codul de procedură civilă",
    "CODUL DE PROCEDURĂ PENALĂ": "Codul de procedură penală",
    "CODUL FISCAL": "Codul fiscal",
    "CODUL MUNCII": "Codul muncii",
    "CODUL DE PROCEDURĂ FISCALĂ": "Codul de procedură fiscală",
    "CODUL SILVIC": "Codul silvic",
    "CODUL VAMAL": "Codul vamal",
    "CODUL AERIAN": "Codul aerian",
    "CODUL COMERCIAL": "Codul comercial",
    "CODUL FAMILIEI": "Codul familiei",
}


def _canonical_citation(
    type_: str | None,
    number: str | None,
    adopted_at,
    issuer: str | None,
) -> str:
    """Build the citation a Romanian lawyer would type. Always returns a string.

    Year comes from `adopted_at` only — published_at (Monitorul Oficial date)
    can shift across years for acts adopted late December, so using it as a
    fallback would silently misattribute ~5 acts/year. Acts with no parseable
    adoption date get a year-less citation like "Ordinul 713" — honest about
    the missing info; the LLM can disambiguate by issuer / title / link.
    """
    if not type_:
        return ""

    if type_ in _SINGLETON_CITATIONS:
        return _SINGLETON_CITATIONS[type_]

    short = _TYPE_SHORTHAND.get(type_, type_.title())
    # "HG" is only correct for government-issued hotărâri. CCR/ÎCCJ hotărâri
    # are a different thing — fall back to "Hotărârea".
    if type_ == "HOTĂRÂRE" and issuer and not issuer.startswith("GUVERNUL"):
        short = "Hotărârea"

    year = adopted_at.year if adopted_at else None
    if number and year:
        # Some sources already carry "287/2009"; don't double-suffix the year.
        return f"{short} {number}" if "/" in number else f"{short} {number}/{year}"
    if number:
        return f"{short} {number}"
    if adopted_at:
        return f"{short} din {adopted_at.isoformat()}"
    return short


def _flush(writer: pq.ParquetWriter, rows: list[dict], schema: pa.Schema) -> None:
    if not rows:
        return
    writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    rows.clear()


def main() -> None:
    logger.info("export: start (input=stdin)")
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
            number = raw.get("Numar")
            issuer = raw.get("Emitent")
            adopted_at = _parse_date(raw.get("AdoptedAt"))

            act_buf.append(
                {
                    "id": act_id,
                    "type": type_,
                    "number": number,
                    "canonical_citation": _canonical_citation(type_, number, adopted_at, issuer),
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

            for article in parsed["articles"]:
                article_id = next_article_id
                next_article_id += 1
                article_buf.append(
                    {
                        "id": article_id,
                        "act_id": act_id,
                        "number": article["number"],
                        "number_variant": article["number_variant"],
                        "full_path": article["full_path"],
                        "content": article["content"],
                    }
                )
                n_articles += 1

                for paragraph in article["paragraphs"]:
                    paragraph_buf.append(
                        {
                            "id": next_paragraph_id,
                            "article_id": article_id,
                            "number": paragraph["number"],
                            "full_path": paragraph["full_path"],
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
                    f"export: progress  acte={n_acts:>7d}  "
                    f"articole={n_articles:>8d}  alineate={n_paragraphs:>9d}"
                )

        _flush(acts_writer, act_buf, ACTS_SCHEMA)
        _flush(articles_writer, article_buf, ARTICLES_SCHEMA)
        _flush(paragraphs_writer, paragraph_buf, PARAGRAPHS_SCHEMA)

    hasher = hashlib.sha256()
    for path in (ACTE_PATH, ARTICOLE_PATH, ALINEATE_PATH):
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
    SHA_PATH.write_text(hasher.hexdigest() + "\n")

    logger.info(f"  acte:     {n_acts:>8d} rows  → {ACTE_PATH}")
    logger.info(f"  articole: {n_articles:>8d} rows  → {ARTICOLE_PATH}")
    logger.info(f"  alineate: {n_paragraphs:>8d} rows  → {ALINEATE_PATH}")
    logger.info(f"  sha256:   {hasher.hexdigest()}")
    logger.success(
        f"export: DONE — {n_acts} acte / {n_articles} articole / {n_paragraphs} alineate"
    )


if __name__ == "__main__":
    main()
