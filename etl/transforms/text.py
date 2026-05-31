"""
Text-cleaning transforms.

Each function takes a LazyFrame and returns a LazyFrame so it composes via
`.pipe()`. Pure-Python helpers are exposed alongside the LazyFrame wrappers
for direct unit-testing of the rule (without spinning up Polars).
"""

import polars as pl

from etl.lookups import load

# Build the translate table once at import. Keys/values are 1-char strings.
_CEDILLA_MAP = load("cedilla")
CEDILLA_TRANSLATE = str.maketrans(_CEDILLA_MAP)
_LEGACY_CHARS = list(_CEDILLA_MAP.keys())
_MODERN_CHARS = list(_CEDILLA_MAP.values())


# ── Pure helpers (testable in isolation) ────────────────────────────────────


def fix_cedilla_str(s: str | None) -> str:
    """Translate legacy ţ/ş/Ţ/Ş → modern ț/ș/Ț/Ș. Empty/None → empty string."""
    return (s or "").translate(CEDILLA_TRANSLATE)


def strip_bom_str(s: str | None) -> str | None:
    """Remove leading U+FEFF byte-order marks. SOAP responses often have them."""
    return s.lstrip("﻿") if s else s


def blank_to_none(s: str | None) -> str | None:
    """Collapse empty / whitespace-only strings to None."""
    if s is None:
        return None
    stripped = s.strip()
    return stripped or None


# ── LazyFrame transforms (composable via .pipe()) ───────────────────────────


def fix_cedilla(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Apply cedilla → comma-below translation to every text column."""
    cols = ["Titlu", "Text", "Emitent", "Publicatie"]
    return lf.with_columns(
        [pl.col(c).str.replace_many(_LEGACY_CHARS, _MODERN_CHARS) for c in cols]
    )


def strip_bom(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Strip leading U+FEFF from text fields the parser later reads."""
    cols = ["Titlu", "Text"]
    return lf.with_columns([pl.col(c).str.strip_chars_start("﻿") for c in cols])


def clean_titlu(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Strip the EMITENT-block suffix SOAP appends to Titlu; collapse whitespace.

    `(?s)` is the inline DOTALL flag — needed so `.*$` consumes the multi-line
    EMITENT block. Polars regex (Rust) doesn't support `re.DOTALL` outside
    inline flags.
    """
    return lf.with_columns(
        pl.col("Titlu")
        .str.replace_all(r"[ \t]*\+[ \t]*", " ")
        .str.replace_all(r"[ \t]+", " ")
        .str.replace_all(r"(?s)\s*EMITENT.*$", "")
        .str.strip_chars()
        .alias("Titlu")
    )


def clean_text(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Collapse whitespace inside Text but preserve newlines (parser needs them)."""
    return lf.with_columns(
        pl.col("Text")
        .str.replace_all(r"[ \t]*\+[ \t]*", " ")
        .str.replace_all(r"[ \t]+", " ")
        .str.replace_all(r"\n[ \t]+", "\n")
        .str.replace_all(r"\n{3,}", "\n\n")
        .str.strip_chars()
        .alias("Text")
    )


def normalize_numar(lf: pl.LazyFrame) -> pl.LazyFrame:
    """SOAP returns '0' when no act number was assigned — convert to null."""
    trimmed = pl.col("Numar").str.strip_chars()
    return lf.with_columns(
        pl.when(trimmed.is_null() | (trimmed == "") | (trimmed == "0"))
        .then(None)
        .otherwise(trimmed)
        .alias("Numar")
    )


def blank_titlu_to_null(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Final pass: empty Titlu → null. Acts with null Titlu get dropped downstream."""
    trimmed = pl.col("Titlu").str.strip_chars()
    return lf.with_columns(
        pl.when(trimmed.is_null() | (trimmed == "")).then(None).otherwise(trimmed).alias("Titlu")
    )
