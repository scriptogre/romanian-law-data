"""
Status transform — derive an act's lifecycle from its actiuniSuferite HTML.

`extract.py` stores, per act, the raw HTML of the site's `actiuniSuferite`
endpoint (what other acts have done TO this one). The act-level rows of that
table state whether the act was repealed or suspended. We read those rows and
collapse them into one word.

    abrogat      an act-level "ABROGAT DE" row, with no later "REPUS"
    suspendat    an act-level "SUSPENDAT DE" row
    în vigoare   none of the above (the act still applies)

A row is act-level when its first cell is exactly "Actul". Per-article rows
("ART. 54 MODIFICAT DE ...") sit in the same table but start with "ART." and
do not change the act's status.

`None` means we never got the HTML (fetch failed, or pre-enrichment data), so
status is unknown. An empty-but-present "Nu exista actiuni" reply is NOT None:
it correctly yields "în vigoare".

The date in a row is the date of the ACTING act, not the date this act stopped
applying. So it does not give a trustworthy `valid_until`. See docs/source-api.md.
"""

import re

import polars as pl

REPEALED = "abrogat"
SUSPENDED = "suspendat"
IN_FORCE = "în vigoare"


def derive_status(suferite_html: str | None) -> str | None:
    """One status word from an actiuniSuferite HTML blob. None if not fetched."""
    if not suferite_html:
        return None

    act_level_ops = []
    for row in re.split(r"</tr>", suferite_html, flags=re.IGNORECASE):
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
    """Add the `Status` column, then drop the raw action HTML.

    Status is computed here, in the main process, so the heavy HTML blobs
    (a code's action list can top 200 KB) never get pickled out to the parse
    workers or written to the final parquet. `actiuni_induse` is dropped too:
    relatii parsing re-reads it straight from raw_acts.jsonl when needed.
    """
    present = set(lf.collect_schema().names())
    if "actiuni_suferite" not in present:
        return lf.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Status"))

    drop = [c for c in ("actiuni_suferite", "actiuni_induse") if c in present]
    return lf.with_columns(
        pl.col("actiuni_suferite")
        .map_elements(derive_status, return_dtype=pl.Utf8)
        .alias("Status")
    ).drop(drop)
