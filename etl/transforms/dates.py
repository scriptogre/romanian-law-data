"""
Date extraction transforms — fully vectorized in Polars.

Three dates govern a Romanian act:
    adopted_at      from Titlu "din DD luna YYYY" (Romanian month name)
    published_at    from Text  "MONITORUL OFICIAL nr. N din DD luna YYYY"
    effective_at    from SOAP  DataVigoare (ISO date string)

All three extractions use Polars expressions — they run on every core via
Polars's Rust/Rayon backend rather than calling into Python per row. The
month-name → number map is applied with `pl.Expr.replace_strict`; the
final date is built via `str.strptime(strict=False)` which yields null
for impossible dates (e.g. Feb 30) instead of raising.
"""

import re

import polars as pl

from etl.lookups import load

ROMANIAN_MONTHS: dict[str, int] = load("romanian_months")
_MONTH_NAMES = list(ROMANIAN_MONTHS.keys())
_MONTH_NUMS = [f"{v:02d}" for v in ROMANIAN_MONTHS.values()]

# Regex patterns — no lookaround, Rust-regex compatible.
_DATE_PATTERN = r"(?i)din\s+(\d{1,2})\s+(" + "|".join(_MONTH_NAMES) + r")\s+(\d{4})"

# MONITORUL OFICIAL phrase. (?s) enables DOTALL so `[^\d]*?` can cross newlines.
_MO_PATTERN = (
    r"(?is)MONITORUL\s+OFICIAL[^\d]*?nr\.\s*([\d.]+)"
    r"(?:\s*bis(?:\s+[IVX]+)?)?"
    r"[^\d]*?din\s+(\d{1,2})\s+(" + "|".join(_MONTH_NAMES) + r")\s+(\d{4})"
)


# ── Pure helpers (kept for direct unit testing) ─────────────────────────────

_DATE_RE_PY = re.compile(
    r"din\s+(?P<day>\d{1,2})\s+(?P<month>" + "|".join(_MONTH_NAMES) + r")\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_MO_RE_PY = re.compile(
    r"MONITORUL\s+OFICIAL[^\d]*?nr\.\s*(?P<number>[\d.]+)"
    r"(?:\s*bis(?:\s+[IVX]+)?)?"
    r"[^\d]*?din\s+(?P<day>\d{1,2})\s+(?P<month>" + "|".join(_MONTH_NAMES) + r")\s+(?P<year>\d{4})",
    re.IGNORECASE | re.DOTALL,
)


def _safe_iso(year: int, month: int, day: int) -> str | None:
    from datetime import date
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def extract_adopted_at_str(titlu: str | None, text: str | None) -> str | None:
    """Adoption date from Titlu, falling back to first 2000 chars of Text."""
    for source in (titlu or "", (text or "")[:2000]):
        m = _DATE_RE_PY.search(source.lower())
        if not m:
            continue
        iso = _safe_iso(
            int(m.group("year")),
            ROMANIAN_MONTHS[m.group("month").lower()],
            int(m.group("day")),
        )
        if iso:
            return iso
    return None


def extract_gazette_str(text: str | None) -> tuple[str | None, int | None]:
    """(mo_publication_date_iso, mo_issue_number) from Text."""
    if not text:
        return (None, None)
    m = _MO_RE_PY.search(text)
    if not m:
        return (None, None)
    iso = _safe_iso(
        int(m.group("year")),
        ROMANIAN_MONTHS[m.group("month").lower()],
        int(m.group("day")),
    )
    try:
        number = int(m.group("number").replace(".", ""))
    except (TypeError, ValueError):
        number = None
    return (iso, number)


def extract_effective_at_str(raw: str | None) -> str | None:
    """SOAP DataVigoare is already ISO YYYY-MM-DD; parse defensively."""
    from datetime import date
    if not raw:
        return None
    try:
        return date.fromisoformat(raw.strip()[:10]).isoformat()
    except ValueError:
        return None


# ── LazyFrame transforms (fully vectorized — no map_elements) ───────────────


def _iso_from_groups(groups: pl.Expr, day_field: str, month_field: str, year_field: str) -> pl.Expr:
    """Build 'YYYY-MM-DD' from a struct of named regex groups. Null if any part missing."""
    month_num = (
        groups.struct.field(month_field).str.to_lowercase()
        .replace_strict(_MONTH_NAMES, _MONTH_NUMS, default=None)
    )
    return pl.concat_str(
        [
            groups.struct.field(year_field),
            pl.lit("-"),
            month_num,
            pl.lit("-"),
            groups.struct.field(day_field).str.pad_start(2, "0"),
        ],
        ignore_nulls=False,
    )


def extract_adopted(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adoption date from Titlu, falling back to first 2000 chars of Text."""
    titlu_match = pl.col("Titlu").fill_null("").str.extract_groups(_DATE_PATTERN)
    text_match = pl.col("Text").fill_null("").str.slice(0, 2000).str.extract_groups(_DATE_PATTERN)
    iso = pl.coalesce(
        [
            _iso_from_groups(titlu_match, "1", "2", "3"),
            _iso_from_groups(text_match, "1", "2", "3"),
        ]
    )
    return lf.with_columns(
        iso.str.strptime(pl.Date, "%Y-%m-%d", strict=False).cast(pl.Utf8).alias("AdoptedAt")
    )


def extract_gazette(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Extract MO publication date + issue number from the Text header."""
    m = pl.col("Text").fill_null("").str.extract_groups(_MO_PATTERN)
    iso = _iso_from_groups(m, "2", "3", "4")
    return lf.with_columns(
        iso.str.strptime(pl.Date, "%Y-%m-%d", strict=False).cast(pl.Utf8).alias("PublishedAt"),
        m.struct.field("1")
        .str.replace_all(r"\.", "")
        .cast(pl.Int64, strict=False)
        .alias("GazetteNumber"),
    )


def extract_effective(lf: pl.LazyFrame) -> pl.LazyFrame:
    """SOAP DataVigoare is already ISO YYYY-MM-DD; defensive parse via strptime."""
    return lf.with_columns(
        pl.col("DataVigoare")
        .fill_null("")
        .str.slice(0, 10)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .cast(pl.Utf8)
        .alias("EffectiveAt")
    )
