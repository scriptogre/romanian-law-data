"""
Relationships transform — parse each act's affects_html into edges.

`affects_html` (the site's actiuniInduse, cached by extracts/relationships.py)
lists what THIS act does to others: rows of [scope, operation, target] with the
target's portal id in the link. We map the Romanian operation to an English
`kind` and emit one row per edge, keyed on portal ids (source = this act, target
= the linked act).

Targets can point OUTSIDE our corpus (old or auxiliary acts), so this is a
standalone edge table, not an FK child of documents.
"""

import re
from pathlib import Path

import polars as pl
from loguru import logger

CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "raw_relationships.parquet"
OUT_PATH = Path(__file__).parent.parent.parent / "data" / "relationships.parquet"

# Romanian operation -> English kind. Matched by keyword, first hit wins; order
# matters (REPUS before PUNE so "REPUS IN VIGOARE" is restore, not enact).
_KIND = [
    ("ABROG", "repeals"),
    ("MODIFIC", "amends"),
    ("COMPLET", "supplements"),
    ("INLOCUI", "replaces"),
    ("RECTIFIC", "corrects"),
    ("NECONSTITUTION", "declares_unconstitutional"),
    ("SUSPEND", "suspends"),
    ("REPUN", "restores"),
    ("REPUS", "restores"),
    ("PUNE IN VIGOARE", "enacts"),
    ("PUNE IN APLICARE", "enacts"),
    ("INCETARE", "terminates"),
    ("INCETEAZA", "terminates"),
    ("PROROG", "extends"),
    ("PRELUNGEST", "extends"),
    ("RESPING", "rejects"),
    ("ELIMIN", "removes"),
    ("REVOC", "revokes"),
    ("ANULEAZ", "annuls"),
]

KINDS = sorted({kind for _, kind in _KIND} | {"other"})

SCHEMA = {
    "source_document_id": pl.Utf8,
    "target_document_id": pl.Utf8,
    "kind": pl.Utf8,
    "partial": pl.Boolean,
    "scope": pl.Utf8,
    "target_citation": pl.Utf8,
}


def map_operation(operation: str) -> tuple[str, bool]:
    """Romanian operation phrase -> (English kind, partial?)."""
    up = operation.upper()
    partial = "PARTIAL" in up
    for key, kind in _KIND:
        if key in up:
            return kind, partial
    return "other", partial


def derive_edges(source_id: str, affects_html: str | None) -> list[dict]:
    """One edge per data row of an affects_html blob. [] if not fetched/empty."""
    if not affects_html:
        return []

    edges: list[dict] = []
    for row in re.split(r"</tr>", affects_html, flags=re.IGNORECASE):
        link = re.search(r"DetaliiDocument/(\d+)", row)
        if not link:
            continue  # header / section-selector rows have no target link
        cells = [
            re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", cell)).strip()
            for cell in re.split(r"</t[dh]>", row, flags=re.IGNORECASE)
        ]
        cells = [c for c in cells if c]
        if len(cells) < 3:
            continue
        scope, operation, target = cells[0], cells[1], cells[2]
        kind, partial = map_operation(operation)
        edges.append(
            {
                "source_document_id": source_id,
                "target_document_id": link.group(1),
                "kind": kind,
                "partial": partial,
                "scope": "act" if scope == "Actul" else scope,
                "target_citation": target,
            }
        )
    return edges


def build_relationships(cache_path: Path = CACHE_PATH, out_path: Path = OUT_PATH) -> int:
    """Parse the relationships cache into the edges parquet. Returns edge count."""
    if not cache_path.exists():
        logger.warning("relationships: no cache present, skipping edges")
        return 0

    cache = pl.read_parquet(cache_path, columns=["document_id", "affects_html"])
    rows: list[dict] = []
    for source_id, affects_html in zip(cache["document_id"], cache["affects_html"]):
        rows.extend(derive_edges(source_id, affects_html))

    pl.DataFrame(rows, schema=SCHEMA).write_parquet(out_path, compression="zstd")
    logger.success(f"relationships: {len(rows)} edges -> {out_path.name}")
    return len(rows)
