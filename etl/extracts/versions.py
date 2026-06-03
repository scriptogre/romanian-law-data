"""
Stage 1 (rider) — extracts/versions.py

The consolidation timeline (`raw_versions`): one row per istoric_fa entry,
parsed from the act-page HTML that extracts/documents.py already downloads.

Not a standalone sweep. The istoric has no endpoint of its own; it exists only
inside the act detail page. So documents.py drives the fetch and hands each
page's HTML here. This module owns the parse, schema, and write of the
document -> versions relation, nothing else.
"""

import re
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw_versions"

FIELDS = ("document_id", "date", "version_id")
SCHEMA = {field: pl.Utf8 for field in FIELDS}


def parse_versions(document_id: int, html: str) -> list[dict]:
    """
    One row per istoric_fa entry, newest-first. `[]` for acts with no history.

    The `istoric_fa` block holds one anchor per consolidation, titled
    `Consolidarea din DD.MM.YYYY`. Past forms link to their own snapshot id
    (`DetaliiDocument/{id}`); the current form has no link, so it carries this
    page's own id.
    """
    rows = []
    for tag in re.finditer(r"<a\b([^>]*)>", html):
        attrs = tag.group(1)
        date = re.search(r"title='Consolidarea din (\d{2}\.\d{2}\.\d{4})'", attrs)
        if not date:
            continue
        link = re.search(r"DetaliiDocument/(\d+)", attrs)
        rows.append(
            {
                "document_id": str(document_id),
                "date": date.group(1),
                "version_id": link.group(1) if link else str(document_id),
            }
        )
    return rows


def write_part(rows: list[dict], shard: int, chunk_start: int) -> None:
    """Write one chunk's version rows to a shard/chunk-named parquet part."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    part = RAW_DIR / f"part_shard{shard:03d}_{chunk_start:08d}.parquet"
    pl.DataFrame(rows, schema=SCHEMA).write_parquet(part, compression="zstd")
