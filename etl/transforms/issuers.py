"""
Emitent (issuer) transforms.

SOAP returns Emitent with non-ASCII chars replaced by `?` ('Curtea
Constitu?ională'). We re-extract the clean form from the EMITENT block at
the head of Text. For joint orders we split with " / ".

The regex uses lookbehind/lookahead which Polars's Rust regex doesn't
support — `recover` therefore uses `map_elements` to call the pure helper
on each row.
"""

import re
import unicodedata

import polars as pl
import yaml

from etl import LOOKUPS_DIR

ISSUER_ALIASES: dict[str, str] = yaml.safe_load(
    (LOOKUPS_DIR / "issuer_aliases.yaml").read_text(encoding="utf-8")
)

# Match "EMITENT  <NAME>" terminated by "Nr." (joint orders) or "Publicat".
EMITENT_RE = re.compile(
    r"EMITENT\s+(?P<name>.+?)\s+(?:Nr\.|Publicat|Republicat)",
    re.IGNORECASE,
)

# Top-level Romanian institutional prefixes. When two appear back-to-back in
# the EMITENT field, the act is a joint order — split with " / " for clarity.
# Uses lookbehind/lookahead → Python re only (not Polars regex).
JOINT_ISSUER_RE = re.compile(
    r"(?<=[a-zăâîșțA-ZĂÂÎȘȚ])\s+"
    r"(?=(?:MINISTERUL|AGENȚIA|AUTORITATEA|CONSILIUL|OFICIUL|CASA\s+NAȚIONALĂ|"
    r"SERVICIUL|BANCA\s+NAȚIONALĂ|ACADEMIA\s+ROMÂNĂ|ÎNALTA\s+CURTE|"
    r"CURTEA\s+CONSTITUȚIONALĂ)\b)"
)


# ── Pure helpers ────────────────────────────────────────────────────────────


def canonicalize_str(name: str | None) -> str | None:
    """Uppercase issuer names. Preserves Romanian diacritics."""
    if not name:
        return name
    return name.upper()


def extract_emitent_str(text: str | None, fallback: str | None) -> str | None:
    """Recover the clean Emitent from Text; canonicalize. Falls back to SOAP value."""
    if not text:
        return canonicalize_str(fallback)
    match = EMITENT_RE.search(text)
    if not match:
        return canonicalize_str(fallback)
    name = unicodedata.normalize("NFC", match.group("name").strip())
    name = re.sub(r"\s{2,}", " ", name)
    name = JOINT_ISSUER_RE.sub(" / ", name)
    return canonicalize_str(name)


# ── LazyFrame transform ─────────────────────────────────────────────────────


def recover_emitent(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Replace Emitent with the clean form extracted from Text's EMITENT block."""
    return lf.with_columns(
        pl.struct(["Text", "Emitent"])
        .map_elements(
            lambda s: extract_emitent_str(s["Text"], s["Emitent"]),
            return_dtype=pl.Utf8,
        )
        .alias("Emitent")
    )


def apply_aliases(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Collapse known Emitent variants to a single canonical form.

    Exact-match replacement: only values present in `issuer_aliases.yaml` are
    rewritten; everything else passes through unchanged. Add entries to the
    YAML as the issuer-distribution audit surfaces new variants.
    """
    if not ISSUER_ALIASES:
        return lf
    return lf.with_columns(
        pl.col("Emitent").replace(ISSUER_ALIASES).alias("Emitent")
    )
