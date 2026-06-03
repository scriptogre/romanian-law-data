"""
Status transform — derive each act's lifecycle from the relationships cache.

extracts/relationships.py stores, per act, the raw HTML of the site's
`actiuniSuferite` endpoint (what other acts have done TO this one) in
`affected_by_html`. Its act-level rows state whether the act was repealed or
suspended; we collapse them into one English status word:

    repealed     an act-level "ABROGAT DE" row, with no later "REPUS"
    suspended    an act-level "SUSPENDAT DE" row
    in_force     none of the above (the act still applies)

A row is act-level when its first cell is exactly "Actul". Per-article rows
("ART. 54 MODIFICAT DE ...") sit in the same table but start with "ART." and do
not change the act's status.

`None` = no HTML for this act (not in the cache), so status is unknown. An
empty-but-present "Nu exista actiuni" reply is NOT None: it yields in_force.

The date in a row is the date of the ACTING act, not when this act stopped
applying, so it does not give a trustworthy `in_force_until`. See docs/source-api.md.
"""

import re
from pathlib import Path

import polars as pl

REPEALED = "repealed"
SUSPENDED = "suspended"
IN_FORCE = "in_force"

CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "raw_relationships.parquet"


def derive_status(affected_by_html: str | None) -> str | None:
    """One status word from an `affected_by_html` blob. None if not fetched."""
    if not affected_by_html:
        return None

    act_level_ops = []
    for row in re.split(r"</tr>", affected_by_html, flags=re.IGNORECASE):
        cells = [
            re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", cell)).strip()
            for cell in re.split(r"</t[dh]>", row, flags=re.IGNORECASE)
        ]
        cells = [cell for cell in cells if cell]
        if len(cells) >= 2 and cells[0] == "Actul":
            act_level_ops.append(cells[1].upper())

    repealed = any("ABROGAT" in op for op in act_level_ops)
    repealed_then_revived = any("REPUS" in op for op in act_level_ops)
    suspended = any("SUSPENDAT" in op for op in act_level_ops)

    if repealed and not repealed_then_revived:
        return REPEALED
    if suspended:
        return SUSPENDED
    return IN_FORCE


def add_status(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add the `Status` column by joining the relationships cache.

    Status is derived from each act's incoming relationships (affected_by_html),
    joined on the portal id in `LinkHtml`. Acts absent from the cache get NULL.
    If the cache isn't present at all (e.g. a build without it), every act is NULL.
    """
    null_status = lf.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Status"))
    if not CACHE_PATH.exists():
        return null_status
    # outgoing-only cache has no affected_by_html; status derivation moves to layer 2
    if "affected_by_html" not in pl.scan_parquet(CACHE_PATH).collect_schema().names():
        return null_status

    status_map = (
        pl.read_parquet(CACHE_PATH, columns=["document_id", "affected_by_html"])
        .with_columns(
            pl.col("affected_by_html")
            .map_elements(derive_status, return_dtype=pl.Utf8)
            .alias("Status")
        )
        .select("document_id", "Status")
        .lazy()
    )

    return (
        lf.with_columns(pl.col("LinkHtml").str.extract(r"(\d+)\s*$", 1).alias("_doc_id"))
        .join(status_map, left_on="_doc_id", right_on="document_id", how="left")
        .drop("_doc_id")
    )
