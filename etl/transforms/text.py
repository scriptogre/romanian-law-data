"""
Text-cleaning transforms.

Each function takes a LazyFrame and returns a LazyFrame so it composes via
`.pipe()`. Pure-Python helpers are exposed alongside the LazyFrame wrappers
for direct unit-testing of the rule (without spinning up Polars).
"""

import re
import unicodedata

import polars as pl
import yaml

from etl import LOOKUPS_DIR

# Build the translate table once at import. Keys/values are 1-char strings.
_CEDILLA_MAP = yaml.safe_load((LOOKUPS_DIR / "cedilla.yaml").read_text(encoding="utf-8"))
CEDILLA_TRANSLATE = str.maketrans(_CEDILLA_MAP)
_LEGACY_CHARS = list(_CEDILLA_MAP.keys())
_MODERN_CHARS = list(_CEDILLA_MAP.values())

_HTML_ENTITY_MAP = yaml.safe_load((LOOKUPS_DIR / "html_entities.yaml").read_text(encoding="utf-8"))
_ENTITY_KEYS = list(_HTML_ENTITY_MAP.keys())
_ENTITY_VALUES = list(_HTML_ENTITY_MAP.values())
# Single-pass alternation for the Python helper. Matches the Polars
# `replace_many` semantics (no iterative re-decoding of `&amp;lt;`).
_ENTITY_RE = re.compile("|".join(re.escape(k) for k in _ENTITY_KEYS))

REPLACEMENT_CHAR = "�"

# Columns that get the full text-cleaning pass. TipAct is included so legacy
# cedilla (CONDIŢII, ORDONANŢĂ MILITARĂ) and any decomposed/entity-encoded
# glyphs there get normalised the same as the rest.
_TEXT_COLS = ["Titlu", "Text", "Emitent", "Publicatie", "TipAct"]


# ── Pure helpers (testable in isolation) ────────────────────────────────────


def fix_cedilla_str(s: str | None) -> str:
    """Translate legacy ţ/ş/Ţ/Ş → modern ț/ș/Ț/Ș. Empty/None → empty string."""
    return (s or "").translate(CEDILLA_TRANSLATE)


def strip_bom_str(s: str | None) -> str | None:
    """Remove leading U+FEFF byte-order marks. SOAP responses often have them."""
    return s.lstrip("﻿") if s else s


def decode_html_entities_str(s: str | None) -> str | None:
    """Decode the entities listed in `data/lookups/html_entities.yaml`.
    Single-pass — `&amp;lt;` becomes `&lt;`, not `<`. Matches `decode_html_entities`.
    """
    if not s:
        return s
    return _ENTITY_RE.sub(lambda m: _HTML_ENTITY_MAP[m.group(0)], s)


def fix_replacement_chars_str(s: str | None) -> str | None:
    """Strip U+FFFD replacement chars left by upstream decoding errors."""
    return s.replace(REPLACEMENT_CHAR, "") if s else s


def normalize_nfc_str(s: str | None) -> str | None:
    """Compose decomposed sequences (e.g. a + combining-breve → ă)."""
    return unicodedata.normalize("NFC", s) if s else s


def blank_to_none(s: str | None) -> str | None:
    """Collapse empty / whitespace-only strings to None."""
    if s is None:
        return None
    stripped = s.strip()
    return stripped or None


# ── LazyFrame transforms (composable via .pipe()) ───────────────────────────


def normalize_nfc(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Compose decomposed unicode sequences. Runs before any rule that
    matches on specific codepoints (cedilla, regex character classes).
    """
    return lf.with_columns([pl.col(c).str.normalize("NFC") for c in _TEXT_COLS])


def decode_html_entities(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Replace `&quot;`, `&amp;`, `&#160;` etc. with their literal characters.
    Single-pass Aho-Corasick — does not iteratively re-decode `&amp;lt;`.
    """
    return lf.with_columns(
        [pl.col(c).str.replace_many(_ENTITY_KEYS, _ENTITY_VALUES) for c in _TEXT_COLS]
    )


def fix_replacement_chars(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Drop U+FFFD replacement characters (encoding noise, no recoverable info)."""
    return lf.with_columns(
        [pl.col(c).str.replace_all(REPLACEMENT_CHAR, "", literal=True) for c in _TEXT_COLS]
    )


def fix_cedilla(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Apply cedilla → comma-below translation to every text column."""
    return lf.with_columns(
        [pl.col(c).str.replace_many(_LEGACY_CHARS, _MODERN_CHARS) for c in _TEXT_COLS]
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
        .str.replace_all(r"[ \t ]*\+[ \t ]*", " ")
        .str.replace_all(r"[ \t ]+", " ")
        .str.replace_all(r"\n[ \t ]+", "\n")
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
