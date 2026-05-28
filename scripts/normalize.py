"""
Stage 2 — normalize.py

Clean up SOAP responses and extract the three distinct dates that govern a
Romanian legal act, plus the Monitorul Oficial issue number.

What gets fixed:

1. Cedilla → comma-below. SOAP `Titlu` uses pre-2007 Romanian orthography
   (`ţ`, `ş`); `Text` already uses the modern forms (`ț`, `ș`). All string
   fields are translated to the modern forms.
2. `Emitent` recovery. SOAP `Emitent` has non-ASCII chars replaced with `?`
   ("Curtea Constitu?ională"). Re-extracted from the `Text` header which
   starts with a clean "EMITENT NAME" block in proper UTF-8. For joint
   orders, captures the FIRST issuer only.
3. `Titlu` cleanup. SOAP appends "EMITENT ... PUBLICAT ÎN ..." after the
   real title; we strip it. Whitespace collapsed.
4. `Numar` placeholder. SOAP returns "0" for acts that have no number;
   stored as NULL.
5. Three dates extracted explicitly:
     adopted_at      from Titlu "din DD luna YYYY"
     published_at    from Text  "MONITORUL OFICIAL nr. N din DD luna YYYY"
     effective_at    from SOAP  DataVigoare (ISO date string)
6. `gazette_number` extracted from the same MO publication phrase.
7. Dedup by normalized Titlu — SOAP returns the same act on adjacent pages
   sometimes. Two acts with identical Titlu are treated as duplicates.

Reads `data/raw_acts.jsonl`, writes `data/normalized_acts.jsonl`.
"""

import json
import re
import unicodedata
from datetime import date
from pathlib import Path

from loguru import logger

INPUT_PATH = Path(__file__).parent.parent / "data" / "raw_acts.jsonl"
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "normalized_acts.jsonl"

# Pre-2007 cedilla forms → post-2007 comma-below forms.
CEDILLA_FIX = str.maketrans(
    {
        "ţ": "ț",  # U+0163 → U+021B
        "ş": "ș",  # U+015F → U+0219
        "Ţ": "Ț",  # U+0162 → U+021A
        "Ş": "Ș",  # U+015E → U+0218
    }
)

ROMANIAN_MONTHS = {
    "ianuarie": 1,
    "februarie": 2,
    "martie": 3,
    "aprilie": 4,
    "mai": 5,
    "iunie": 6,
    "iulie": 7,
    "august": 8,
    "septembrie": 9,
    "octombrie": 10,
    "noiembrie": 11,
    "decembrie": 12,
}

# "din DD luna YYYY" — used for adopted_at extraction from Titlu.
DATE_RE = re.compile(
    r"din\s+(?P<day>\d{1,2})\s+(?P<month>" + "|".join(ROMANIAN_MONTHS) + r")\s+(?P<year>\d{4})",
    re.IGNORECASE,
)

# "MONITORUL OFICIAL nr. <number> din DD luna YYYY" — gazette + publication date.
# Number may use Romanian thousands separator: "nr. 1.216". Optional suffix "bis"
# / "bis I" denotes a supplementary issue with the same number.
MO_RE = re.compile(
    r"MONITORUL\s+OFICIAL[^\d]*?nr\.\s*(?P<number>[\d.]+)"
    r"(?:\s*bis(?:\s+[IVX]+)?)?"
    r"[^\d]*?din\s+(?P<day>\d{1,2})\s+(?P<month>"
    + "|".join(ROMANIAN_MONTHS)
    + r")\s+(?P<year>\d{4})",
    re.IGNORECASE | re.DOTALL,
)

# Match "EMITENT  <NAME>" terminated by "Nr." (joint orders) or "Publicat".
EMITENT_RE = re.compile(
    r"EMITENT\s+(?P<name>.+?)\s+(?:Nr\.|Publicat|Republicat)",
    re.IGNORECASE,
)

# Top-level Romanian institutional prefixes. When two appear back-to-back in
# the EMITENT field, the act is a joint order — split with " / " for clarity.
JOINT_ISSUER_RE = re.compile(
    r"(?<=[a-zăâîșțA-ZĂÂÎȘȚ])\s+"
    r"(?=(?:MINISTERUL|AGENȚIA|AUTORITATEA|CONSILIUL|OFICIUL|CASA\s+NAȚIONALĂ|"
    r"SERVICIUL|BANCA\s+NAȚIONALĂ|ACADEMIA\s+ROMÂNĂ|ÎNALTA\s+CURTE|"
    r"CURTEA\s+CONSTITUȚIONALĂ)\b)"
)


def fix_cedilla(value: str | None) -> str:
    return (value or "").translate(CEDILLA_FIX)


def strip_bom(value: str) -> str:
    """Remove leading byte-order marks. SOAP responses often start with U+FEFF."""
    return value.lstrip("﻿") if value else value


def blank_to_none(value: str | None) -> str | None:
    """Collapse empty / whitespace-only strings to None."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def number_to_none(value: str | None) -> str | None:
    """Drop SOAP placeholders. '0' means 'no number assigned'."""
    cleaned = blank_to_none(value)
    return None if cleaned == "0" else cleaned


def clean_inline_whitespace(value: str) -> str:
    """Collapse horizontal whitespace and stray `+` markers."""
    value = re.sub(r"[ \t]*\+[ \t]*", " ", value)
    value = re.sub(r"[ \t]+", " ", value)
    return value.strip()


def strip_titlu_suffix(value: str) -> str:
    """Cut the document-header suffix that SOAP appends to Titlu."""
    return re.sub(r"\s*EMITENT.*$", "", value, flags=re.DOTALL).strip()


def clean_text(value: str) -> str:
    """Like clean_inline_whitespace but preserves newlines (parser needs them)."""
    value = re.sub(r"[ \t]*\+[ \t]*", " ", value)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n[ \t]+", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def extract_emitent(text: str, fallback: str) -> str:
    if not text:
        return canonicalize_emitent(fallback)
    match = EMITENT_RE.search(text)
    if not match:
        return canonicalize_emitent(fallback)
    name = unicodedata.normalize("NFC", match.group("name").strip())
    name = re.sub(r"\s{2,}", " ", name)
    name = JOINT_ISSUER_RE.sub(" / ", name)
    return canonicalize_emitent(name)


def canonicalize_emitent(name: str) -> str:
    """Uppercase issuer names. SOAP returns mixed case (mostly uppercase) for
    the same institutions ('GUVERNUL' vs 'Guvernul'). Uppercasing is the
    cheapest canonicalization that still preserves Romanian diacritics.
    """
    if not name:
        return name
    return name.upper()


def _safe_date(year: int, month: int, day: int) -> str | None:
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def extract_adopted_at(titlu: str, text: str) -> str | None:
    """Extract the act's adoption / signing date from the title (`din DATE`).

    Title is the primary source. When absent (a few thousand acts where SOAP's
    Titlu doesn't carry the date — typically ORDIN-uri), fall back to the head
    of Text, where the official header still starts with "TIP nr. N din DATE".
    2_000 chars is generous enough to clear the EMITENT block + header but
    short enough to avoid catching dates from cited acts in the body.
    """
    for source in (titlu or "", (text or "")[:2000]):
        match = DATE_RE.search(source.lower())
        if not match:
            continue
        result = _safe_date(
            int(match.group("year")),
            ROMANIAN_MONTHS[match.group("month").lower()],
            int(match.group("day")),
        )
        if result:
            return result
    return None


def extract_gazette(text: str) -> tuple[str | None, int | None]:
    """Extract (mo_publication_date_iso, mo_issue_number) from the Text header."""
    if not text:
        return (None, None)
    match = MO_RE.search(text)
    if not match:
        return (None, None)
    iso = _safe_date(
        int(match.group("year")),
        ROMANIAN_MONTHS[match.group("month").lower()],
        int(match.group("day")),
    )
    raw_number = (match.group("number") or "").replace(".", "")
    try:
        number = int(raw_number)
    except (TypeError, ValueError):
        number = None
    return (iso, number)


def extract_effective_at(raw_data_vigoare: str | None) -> str | None:
    """SOAP DataVigoare is already ISO YYYY-MM-DD. Parse defensively."""
    if not raw_data_vigoare:
        return None
    s = raw_data_vigoare.strip()[:10]
    try:
        return date.fromisoformat(s).isoformat()
    except ValueError:
        return None


def normalize(act: dict) -> dict:
    titlu = strip_titlu_suffix(clean_inline_whitespace(strip_bom(fix_cedilla(act.get("Titlu")))))
    text = clean_text(strip_bom(fix_cedilla(act.get("Text"))))
    emitent = extract_emitent(text, fix_cedilla(act.get("Emitent") or ""))

    adopted_at = extract_adopted_at(titlu, text)
    published_at_str, gazette_number = extract_gazette(text)
    effective_at = extract_effective_at(act.get("DataVigoare"))

    return {
        "Titlu": blank_to_none(titlu),
        "Text": blank_to_none(text),
        "TipAct": blank_to_none(act.get("TipAct")),
        "Numar": number_to_none(act.get("Numar")),
        "Emitent": blank_to_none(emitent),
        "Publicatie": blank_to_none(fix_cedilla(act.get("Publicatie"))),
        "LinkHtml": blank_to_none(act.get("LinkHtml")),
        "AdoptedAt": adopted_at,
        "PublishedAt": published_at_str,
        "EffectiveAt": effective_at,
        "GazetteNumber": gazette_number,
    }


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    duplicates_dropped = 0
    emitent_recovered = 0
    adopted_extracted = 0
    published_extracted = 0
    effective_extracted = 0
    gazette_extracted = 0
    number_nulled = 0
    seen_keys: set[tuple[str, str | None]] = set()

    with INPUT_PATH.open(encoding="utf-8") as src, OUTPUT_PATH.open("w", encoding="utf-8") as dst:
        for line in src:
            act = json.loads(line)
            original_emitent = act.get("Emitent") or ""
            original_numar = (act.get("Numar") or "").strip()
            normalized = normalize(act)

            titlu = normalized.get("Titlu")
            if titlu is None:
                total += 1
                continue
            # (Titlu, Emitent) catches both:
            #   • SOAP page duplicates (same title + emitter)
            #   • Distinct filings under boilerplate titles (CUANTUM TOTAL —
            #     one row per political party, same title, different party in
            #     Emitent extracted from Text header).
            key = (titlu, normalized.get("Emitent"))
            if key in seen_keys:
                duplicates_dropped += 1
                total += 1
                continue
            seen_keys.add(key)

            emitent = normalized.get("Emitent") or ""
            if "?" in original_emitent and "?" not in emitent:
                emitent_recovered += 1
            if normalized.get("AdoptedAt"):
                adopted_extracted += 1
            if normalized.get("PublishedAt"):
                published_extracted += 1
            if normalized.get("EffectiveAt"):
                effective_extracted += 1
            if normalized.get("GazetteNumber") is not None:
                gazette_extracted += 1
            if original_numar and normalized.get("Numar") is None:
                number_nulled += 1

            dst.write(json.dumps(normalized, ensure_ascii=False) + "\n")
            total += 1

    unique = total - duplicates_dropped
    logger.success(f"normalized {unique} unique acts → {OUTPUT_PATH}")
    logger.info(f"  duplicates dropped:           {duplicates_dropped} / {total}")
    logger.info(f"  emitent recovered from Text:  {emitent_recovered} / {unique}")
    logger.info(f"  adopted_at extracted:         {adopted_extracted} / {unique}")
    logger.info(f"  published_at (MO) extracted:  {published_extracted} / {unique}")
    logger.info(f"  effective_at extracted:       {effective_extracted} / {unique}")
    logger.info(f"  gazette_number extracted:     {gazette_extracted} / {unique}")
    logger.info(f"  number placeholders → NULL:   {number_nulled} / {unique}")


if __name__ == "__main__":
    main()
