"""
Stage 2 — transform.py

Clean each raw SOAP act, then slice its body into articles and paragraphs.
Reads `data/raw_acts.jsonl` (one JSON object per line, as written by
extract.py); writes parsed acts to stdout for load.py to pipe-consume.

Each output line has the shape:

    {
        "raw": { ...normalized act fields... },
        "articles": [
            {
                "number": 188,                          # INTEGER
                "number_variant": null | "bis" | "^1",  # VARCHAR NULL
                "full_path": "Art. 188",
                "content": "...",
                "paragraphs": [
                    {
                        "number": 1 | null,             # null = whole article, no alineate
                        "full_path": "Art. 188 alin. (1)",
                        "content": "..."
                    },
                    ...
                ]
            },
            ...
        ],
        "quality": { score, band, signals, ... }
    }

The per-act diagnostic report still writes to `data/parse_report.jsonl`.

What the normalize-half fixes in the raw SOAP response:

1. Cedilla → comma-below. SOAP `Titlu` uses pre-2007 Romanian orthography
   (`ţ`, `ş`); `Text` already uses the modern forms. Translated via
   `data/lookups/cedilla.yaml`.
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
7. Dedup by (normalized Titlu, Emitent) — SOAP returns the same act on
   adjacent pages sometimes.

What the parse-half does on the cleaned text:

1. Slices text into articles by `ARTICLE_RE` markers; falls back to a
   single "(unparsed)" article holding the whole text when no markers match.
2. Slices each article into paragraphs by `PARAGRAPH_RE`; falls back to one
   NULL-numbered paragraph holding the whole article.
3. Scores each act on five signals (coverage, detection_recall, body_sanity,
   paragraph_contiguity, no_tail_orphan) with a detection-recall gate that
   caps the band when articles were silently dropped.

Quality bands reported at end of run:
    high quality           (≥0.85)
    medium quality         (0.50 – 0.85)
    low quality            (<0.50)
    intentional fallback   — no article markers AND not expected to have any
"""

import json
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path

import yaml
from loguru import logger

REPO_ROOT = Path(__file__).parent.parent
INPUT_PATH = REPO_ROOT / "data" / "raw_acts.jsonl"
REPORT_PATH = REPO_ROOT / "data" / "parse_report.jsonl"
LOOKUPS_DIR = REPO_ROOT / "data" / "lookups"


def _load_yaml(name: str) -> dict:
    with (LOOKUPS_DIR / f"{name}.yaml").open(encoding="utf-8") as f:
        return yaml.safe_load(f)


# Pre-2007 cedilla → modern comma-below diacritics. Loaded as a translate table.
CEDILLA_FIX = str.maketrans(_load_yaml("cedilla"))

ROMANIAN_MONTHS: dict[str, int] = _load_yaml("romanian_months")


# ── Normalize-half: regex patterns ──────────────────────────────────────────

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


# ── Normalize-half: text cleaning ───────────────────────────────────────────


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


# ── Normalize-half: emitent extraction ──────────────────────────────────────


def canonicalize_emitent(name: str) -> str:
    """Uppercase issuer names. SOAP returns mixed case (mostly uppercase) for
    the same institutions ('GUVERNUL' vs 'Guvernul'). Uppercasing is the
    cheapest canonicalization that still preserves Romanian diacritics.
    """
    if not name:
        return name
    return name.upper()


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


# ── Normalize-half: date extraction ─────────────────────────────────────────


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


# ── Normalize-half: top-level ───────────────────────────────────────────────


def normalize_act(act: dict) -> dict:
    """Clean a single raw SOAP act and extract its structured fields."""
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


# ── Parse-half: regex patterns ──────────────────────────────────────────────

MIN_ARTICLES_FOR_CLEAN_PARSE = 1
TINY_BODY_THRESHOLD = 20  # below this many chars, an article body is suspicious

QUALITY_WEIGHTS = {
    "coverage": 0.30,
    "detection_recall": 0.25,
    "body_sanity": 0.20,
    "paragraph_contiguity": 0.15,
    "no_tail_orphan": 0.10,
}

# Quality band thresholds.
HIGH_QUALITY = 0.85
MEDIUM_QUALITY = 0.50

# Detection-recall gate. Catches silent article drops (e.g. annex truncation
# absorbing real articles) that the weighted composite would dilute. Caps the
# band at the floor below the corresponding threshold, regardless of how well
# the in-scope articles scored on coverage / sanity / etc.
DETECTION_RECALL_LOW_GATE = 0.50  # below → band = "low"
DETECTION_RECALL_MEDIUM_GATE = 0.85  # below → band capped at "medium"

# A "raw article marker" — used for the count-only regex below. Looser than
# ARTICLE_RE on purpose: we want to know how many candidate markers the text
# CONTAINS, even if some weren't extracted (so we can tell when we miss).
RAW_MARKER_RE = re.compile(
    r"(?:^|\n)[ \t]*(?:Articolul|ARTICOLUL|Art\.|ART\.)[ \t]+(?:\d+|[IVXLCDM]+\b)",
)

# Annex header. Real annexes are identified by article-number continuity, not
# just by pattern match — `Anexa nr. 1` also appears inline as a cross-reference
# inside article bodies (e.g. Cod Fiscal art. 24 references "Anexa nr. 1") and
# would falsely truncate scope. See `_find_real_annex` below.
ANNEX_BOUNDARY_RE = re.compile(
    r"(?:^|\n)\s*(?:ANEX[ĂA]|Anex[ăa])"
    r"(?:\s+(?:NR\.|nr\.|\d|[IVX]+|la)|\s*\Z)",
)

# Signing block — appears at the end of most Romanian acts, after the last
# article body. Excluded from coverage / orphan-tail calculations.
SIGNING_BLOCK_RE = re.compile(
    r"(?:^|\n)\s*("
    r"PREȘEDINTELE\s+ROMÂNIEI"
    r"|PRIM[-‑]MINISTRU(?:L)?"
    r"|PREȘEDINTELE\s+CAMEREI\s+DEPUTAȚILOR"
    r"|PREȘEDINTELE\s+SENATULUI"
    r"|p\.\s+MINISTRUL"
    r"|MINISTRUL\s+[A-ZĂÂÎȘȚ]"
    r"|GUVERNATORUL\s+BĂNCII"
    r"|PREȘEDINTELE\s+CURȚII"
    r")\b",
)

# Any of the recognized markers — numbered articles OR "Articol unic".
ZONE_START_RE = re.compile(
    r"(?:^|\n)[ \t]*("
    r"Articolul|ARTICOLUL|Art\.|ART\."
    r"|Articol\s+unic|ARTICOL\s+UNIC|Articolul\s+unic"
    r")\b",
)

# DECRETs and other short acts often use "Articol unic" / "ARTICOL UNIC"
# instead of numbered articles. Treat as a single article with number=NULL
# and full_path = "Articol unic".
UNIQUE_ARTICLE_RE = re.compile(
    r"(?:^|\n)[ \t]*(?:Articol\s+unic|ARTICOL\s+UNIC|Articolul\s+unic)\b[ \t]*",
)

# Match the article marker ONLY (not the body). Body is the slice between
# successive matches.
#
# Romanian legal text uses these keyword forms at line start:
#   "Articolul 1"      — modern, most common
#   "Articolul I"      — amending laws use Roman numerals to enumerate steps
#   "ARTICOLUL 1"      — older / formal
#   "Art. 188"         — older / formal
#   "ART. 188"         — older / formal
#
# Case-sensitive on the keyword to avoid false matches on "art. 188" cross-
# references inside body text. The body often continues on the same line as
# the marker, so we do NOT consume past the article number / variant.
#
# Roman numerals get normalized to int via roman_to_int() so `number` stays
# sortable; `full_path` preserves the original Roman rendering for display.
#
# Known limitation: dotted hierarchical numbering ("Articolul 1.1.1") used by
# technical regulations (REGLEMENTĂRI TEHNICE) and contract templates inside
# guides (GHID) is NOT matched. These acts fall back to "(unparsed)" with full
# text preserved. Supporting them requires a schema change since `number` is
# int. Affects ~0.02% of acts.
ARTICLE_RE = re.compile(
    r"""
        (?:^|\n)[ \t]*
        (?:Articolul|ARTICOLUL|Art\.|ART\.)
        [ \t]+
        (?:
            (?P<number>\d+(?:\.\d{3})*)
            (?:
                [ \t]+(?P<variant_latin>bis|ter|quater|quinquies|sexies|septies|octies|Bis|Ter|BIS|TER)
                |
                [ \t]*\^[ \t]*(?P<variant_super>\d+)
            )?
            |
            (?P<roman>(?:M{0,3}(?:CM|CD|D?C{0,3})(?:XC|XL|L?X{0,3})(?:IX|IV|V?I{1,3})|V|X|L|C|D|M))
        )
        \.?
        (?:\*+\))?              # optional footnote ref e.g. "Articolul 200*)"
        (?=[ \t]|$)
    """,
    re.VERBOSE,
)

# Paragraphs: "(1)", "(2)" anywhere they appear as a paragraph marker.
# Inline form is common: ". (1) Administrația ... (2) Activitățile ...".
#
# Disambiguation from cross-references like "alin. (1) se aplică":
#   - Real paragraph: "(N)" preceded by whitespace, followed by space + uppercase
#   - Cross-reference: "(N)" preceded by "alin." or similar, followed by lowercase
#
# We require start-of-string or preceding whitespace / sentence punctuation,
# and an uppercase Romanian letter following the marker.
PARAGRAPH_RE = re.compile(
    r"""
        (?:^|(?<=[\s.;,!?]))
        \((?P<number>\d+)\)
        [ \t]+
        (?=[A-ZĂÂÎȘȚ])
    """,
    re.VERBOSE,
)


# ── Parse-half: article extraction ──────────────────────────────────────────

ROMAN_VALUES = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}


def roman_to_int(s: str) -> int:
    """Convert a Roman numeral string to int. Permissive on subtractive form."""
    total = 0
    prev = 0
    for ch in reversed(s.upper()):
        v = ROMAN_VALUES[ch]
        if v < prev:
            total -= v
        else:
            total += v
        prev = v
    return total


def _format_article_path(number: int, variant: str | None, roman: str | None = None) -> str:
    label = roman or str(number)
    if not variant:
        return f"Art. {label}"
    if variant.startswith("^") or variant.isdigit():
        return f"Art. {label}^{variant.lstrip('^')}"
    return f"Art. {label} {variant}"


def _article_number(match: re.Match) -> int | None:
    """Numeric article number from a match (strips Romanian thousands separator).
    None for Roman-numeral matches — caller resolves via roman_to_int().
    """
    raw = match.group("number")
    return int(raw.replace(".", "")) if raw else None


def _find_real_annex(text: str, all_matches: list[re.Match]) -> int:
    """First real-annex position, or len(text).

    `ANNEX_BOUNDARY_RE` matches inline body cross-references too. Real annexes
    are detected by article-number continuity: if any number AFTER a candidate
    is a continuation (> the running max BEFORE), the candidate is an inline
    reference. Otherwise it's a real annex (numbers restart, or no articles
    follow at all).
    """
    candidates = list(ANNEX_BOUNDARY_RE.finditer(text))
    if not candidates:
        return len(text)
    if not all_matches:
        return candidates[0].start()

    for candidate in candidates:
        pos = candidate.start()
        before = [
            n for m in all_matches if m.start() < pos and (n := _article_number(m)) is not None
        ]
        after = [
            n for m in all_matches if m.start() > pos and (n := _article_number(m)) is not None
        ]
        if not after:
            return pos
        if before and any(n > max(before) for n in after):
            continue
        return pos
    return len(text)


def _extract_articles(text: str) -> list[dict]:
    """Slice plain text into articles. Returns ordered list of article dicts.

    Annex region is excluded so template articles inside annexes don't pollute
    the parent act's numbering. See `_find_real_annex` for the heuristic.
    """
    all_matches = list(ARTICLE_RE.finditer(text))
    annex_pos = _find_real_annex(text, all_matches)
    scope = text[:annex_pos]
    matches = [m for m in all_matches if m.start() < annex_pos]

    if not matches:
        unique = UNIQUE_ARTICLE_RE.search(scope)
        if unique:
            return [
                {
                    "number": None,
                    "number_variant": None,
                    "full_path": "Articol unic",
                    "content": scope[unique.end() :].strip(),
                }
            ]
        return []

    articles: list[dict] = []
    for i, match in enumerate(matches):
        roman = match.group("roman")
        if roman:
            number = roman_to_int(roman)
            variant = None
        else:
            number = _article_number(match)
            latin = match.group("variant_latin")
            variant = (
                latin.lower()
                if latin
                else f"^{match.group('variant_super')}"
                if match.group("variant_super")
                else None
            )
        body_start = match.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(scope)
        content = scope[body_start:body_end].strip()

        articles.append(
            {
                "number": number,
                "number_variant": variant,
                "full_path": _format_article_path(number, variant, roman=roman),
                "content": content,
            }
        )
    return articles


# ── Parse-half: paragraph extraction ────────────────────────────────────────


def _extract_paragraphs(article_path: str, content: str) -> list[dict]:
    """Slice an article's text into alineate. Falls back to single NULL-numbered paragraph."""
    matches = list(PARAGRAPH_RE.finditer(content))
    if not matches:
        return [
            {
                "number": None,
                "full_path": article_path,
                "content": content.strip(),
            }
        ]

    paragraphs: list[dict] = []
    # Capture any prologue text before the first (1) marker — usually empty / header.
    prologue = content[: matches[0].start()].strip()
    if prologue:
        paragraphs.append(
            {
                "number": None,
                "full_path": article_path,
                "content": prologue,
            }
        )

    for i, match in enumerate(matches):
        number = int(match.group("number"))
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        body = content[start:end].strip()
        paragraphs.append(
            {
                "number": number,
                "full_path": f"{article_path} alin. ({number})",
                "content": body,
            }
        )
    return paragraphs


# ── Parse-half: quality scoring ─────────────────────────────────────────────


def _structural_zone(text: str) -> tuple[int, int]:
    """Return (start, end) of the articulated portion of the text.

    start = position of first article marker — numbered OR "Articol unic"
            (or 0 if none).
    end   = earliest of: first ANEX[ĂA] header, first signing block, or
            end of text. Truncates preamble, annexes, and signing blocks so
            coverage / orphan-tail signals measure only the articulated zone.
    """
    first = ZONE_START_RE.search(text)
    start = first.start() if first else 0
    candidates = [len(text)]
    annex = ANNEX_BOUNDARY_RE.search(text, start)
    if annex:
        candidates.append(annex.start())
    signing = SIGNING_BLOCK_RE.search(text, start)
    if signing:
        candidates.append(signing.start())
    return (start, min(candidates))


def _coverage(text: str, articles: list[dict]) -> float:
    """Fraction of the articulated zone captured by article bodies. 0.0 – 1.0.

    Denominator excludes preamble (text before first marker) and annexes
    (everything from the first ANEXĂ heading onwards). What remains is the
    region the parser is responsible for; we expect article bodies to cover
    most of it.
    """
    if not text:
        return 0.0
    captured = sum(len(a.get("content") or "") for a in articles)
    start, end = _structural_zone(text)
    zone = max(1, end - start)
    return min(1.0, captured / zone)


def _detection_recall(text: str, articles: list[dict]) -> float:
    """detected_articles / expected_markers, capped at 1.0.

    expected = raw "Articolul N" / "Art. N" / "ARTICOLUL N" / "ART. N" markers
    in the source text. If the text uses ARTICOL UNIC (no numbered markers),
    we treat expected=1 and detected=1 if the article exists.
    """
    expected = len(RAW_MARKER_RE.findall(text or ""))
    if expected == 0:
        if articles and articles[0].get("full_path") == "Articol unic":
            return 1.0
        return 1.0 if not articles else 0.0
    return min(1.0, len(articles) / expected)


def _body_sanity(articles: list[dict]) -> float:
    """Fraction of articles with content of at least TINY_BODY_THRESHOLD chars."""
    if not articles:
        return 0.0
    healthy = sum(1 for a in articles if len(a.get("content") or "") >= TINY_BODY_THRESHOLD)
    return healthy / len(articles)


def _paragraph_contiguity(articles: list[dict]) -> float:
    """Fraction of multi-paragraph articles where alineat numbers form 1, 2, 3, ...

    Articles with a single (NULL-numbered) paragraph are skipped — they don't
    have alineate to validate. Returns 1.0 if no articles qualify (nothing to
    penalize).
    """
    qualifying = 0
    contiguous = 0
    for a in articles:
        nums = [p["number"] for p in a.get("paragraphs", []) if p.get("number") is not None]
        if len(nums) < 2:
            continue
        qualifying += 1
        if nums == list(range(1, len(nums) + 1)):
            contiguous += 1
    if qualifying == 0:
        return 1.0
    return contiguous / qualifying


def _no_tail_orphan(text: str, articles: list[dict]) -> float:
    """1.0 minus (orphan chars / zone size). 1.0 = no orphan.

    "Orphan" = uncovered text inside the structural zone (after the start of
    the first article marker, before any ANEXĂ heading). Annexes are NOT
    counted as orphan; they're explicitly out-of-scope for article parsing.
    """
    if not text:
        return 1.0
    if not articles:
        return 0.0
    last = articles[-1].get("content") or ""
    if not last:
        return 1.0
    last_end = text.rfind(last)
    if last_end < 0:
        return 1.0
    last_end += len(last)
    start, zone_end = _structural_zone(text)
    if last_end >= zone_end:
        return 1.0
    orphan_chars = zone_end - last_end
    zone = max(1, zone_end - start)
    return max(0.0, 1.0 - orphan_chars / zone)


def compute_quality(text: str, articles: list[dict], *, is_fallback: bool) -> dict:
    """Per-act quality signals + composite score.

    Returns:
        {
          "score": 0.0–1.0,
          "band": "high" | "medium" | "low" | "intentional-fallback",
          "signals": { coverage, detection_recall, body_sanity, ... },
          "expected_markers": int,
          "detected_articles": int,
        }
    """
    expected_markers = len(RAW_MARKER_RE.findall(text or ""))

    if is_fallback:
        # No structured articles extracted. Distinguish "no markers in source"
        # (intentional, e.g. RAPORT / COMUNICAT) from "had markers, we missed
        # them" (real parser miss).
        if expected_markers == 0:
            return {
                "score": 1.0,
                "band": "intentional-fallback",
                "signals": {k: None for k in QUALITY_WEIGHTS},
                "expected_markers": 0,
                "detected_articles": 0,
            }
        return {
            "score": 0.0,
            "band": "low",
            "signals": {k: 0.0 for k in QUALITY_WEIGHTS},
            "expected_markers": expected_markers,
            "detected_articles": 0,
        }

    signals = {
        "coverage": _coverage(text, articles),
        "detection_recall": _detection_recall(text, articles),
        "body_sanity": _body_sanity(articles),
        "paragraph_contiguity": _paragraph_contiguity(articles),
        "no_tail_orphan": _no_tail_orphan(text, articles),
    }
    score = sum(signals[k] * w for k, w in QUALITY_WEIGHTS.items())
    band = "high" if score >= HIGH_QUALITY else "medium" if score >= MEDIUM_QUALITY else "low"

    recall = signals["detection_recall"]
    gate = None
    if recall < DETECTION_RECALL_LOW_GATE:
        band, gate = "low", "detection_recall_low"
    elif recall < DETECTION_RECALL_MEDIUM_GATE and band == "high":
        band, gate = "medium", "detection_recall_medium"

    return {
        "score": round(score, 4),
        "band": band,
        "gate": gate,
        "signals": {k: round(v, 4) for k, v in signals.items()},
        "expected_markers": expected_markers,
        "detected_articles": len(articles),
    }


# ── Parse-half: top-level ───────────────────────────────────────────────────


def parse_act(act: dict) -> dict:
    """Split a normalized act's text into articles + paragraphs + quality."""
    text = act.get("Text") or ""
    articles = _extract_articles(text)
    is_fallback = len(articles) < MIN_ARTICLES_FOR_CLEAN_PARSE

    if is_fallback:
        # Fallback: one synthetic article holding the whole text. Lets the act
        # remain queryable while we tune the parser.
        articles = [
            {
                "number": None,
                "number_variant": None,
                "full_path": "(unparsed)",
                "content": text.strip(),
            }
        ]

    for article in articles:
        article["paragraphs"] = _extract_paragraphs(article["full_path"], article["content"])

    quality = compute_quality(text, articles if not is_fallback else [], is_fallback=is_fallback)
    return {"raw": act, "articles": articles, "quality": quality}


def transform_act(raw_act: dict) -> dict:
    """End-to-end transform: raw SOAP dict → normalized + parsed + scored."""
    return parse_act(normalize_act(raw_act))


# ── Driver ──────────────────────────────────────────────────────────────────


def main() -> None:
    logger.info(f"transform: start (input={INPUT_PATH.name})")
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    duplicates_dropped = 0
    emitent_recovered = 0
    adopted_extracted = 0
    published_extracted = 0
    effective_extracted = 0
    gazette_extracted = 0
    number_nulled = 0
    bands: dict[str, int] = {"high": 0, "medium": 0, "low": 0, "intentional-fallback": 0}
    gates: dict[str, int] = {"detection_recall_low": 0, "detection_recall_medium": 0}
    score_sum = 0.0
    seen_keys: set[tuple[str, str | None]] = set()

    with INPUT_PATH.open(encoding="utf-8") as src, REPORT_PATH.open("w", encoding="utf-8") as report:
        for line in src:
            if total and total % 10_000 == 0:
                avg = score_sum / max(1, total - duplicates_dropped)
                logger.info(
                    f"transform: progress  read={total:>7d}  "
                    f"unique={total - duplicates_dropped:>7d}  "
                    f"dupes={duplicates_dropped:>5d}  "
                    f"high={bands['high']:>5d}  med={bands['medium']:>4d}  "
                    f"low={bands['low']:>4d}  fallback={bands['intentional-fallback']:>5d}  "
                    f"mean_score={avg:.3f}"
                )
            raw_act = json.loads(line)
            total += 1

            original_emitent = raw_act.get("Emitent") or ""
            original_numar = (raw_act.get("Numar") or "").strip()
            normalized = normalize_act(raw_act)

            titlu = normalized.get("Titlu")
            if titlu is None:
                continue
            # (Titlu, Emitent) catches both:
            #   • SOAP page duplicates (same title + emitter)
            #   • Distinct filings under boilerplate titles (CUANTUM TOTAL —
            #     one row per political party, same title, different party in
            #     Emitent extracted from Text header).
            key = (titlu, normalized.get("Emitent"))
            if key in seen_keys:
                duplicates_dropped += 1
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

            parsed = parse_act(normalized)
            sys.stdout.write(json.dumps(parsed, ensure_ascii=False) + "\n")

            quality = parsed["quality"]
            bands[quality["band"]] += 1
            score_sum += quality["score"]
            if quality.get("gate"):
                gates[quality["gate"]] += 1

            report.write(
                json.dumps(
                    {
                        "title": normalized.get("Titlu"),
                        "type": normalized.get("TipAct"),
                        "number": normalized.get("Numar"),
                        "text_length": len(normalized.get("Text") or ""),
                        "score": quality["score"],
                        "band": quality["band"],
                        "gate": quality.get("gate"),
                        "signals": quality["signals"],
                        "expected_markers": quality["expected_markers"],
                        "detected_articles": quality["detected_articles"],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    unique = total - duplicates_dropped
    avg = score_sum / unique if unique else 0.0
    logger.info(f"  duplicates dropped:           {duplicates_dropped} / {total}")
    logger.info(f"  emitent recovered from Text:  {emitent_recovered} / {unique}")
    logger.info(f"  adopted_at extracted:         {adopted_extracted} / {unique}")
    logger.info(f"  published_at (MO) extracted:  {published_extracted} / {unique}")
    logger.info(f"  effective_at extracted:       {effective_extracted} / {unique}")
    logger.info(f"  gazette_number extracted:     {gazette_extracted} / {unique}")
    logger.info(f"  number placeholders → NULL:   {number_nulled} / {unique}")
    logger.info(f"  mean quality score:           {avg:.3f}")
    logger.info(
        f"  high   (≥{HIGH_QUALITY}):           {bands['high']:>6d} ({bands['high'] / unique:.1%})"
    )
    logger.info(
        f"  medium ({MEDIUM_QUALITY}–{HIGH_QUALITY}):       {bands['medium']:>6d} ({bands['medium'] / unique:.1%})"
    )
    logger.info(
        f"  low    (<{MEDIUM_QUALITY}):           {bands['low']:>6d} ({bands['low'] / unique:.1%})"
    )
    logger.info(
        f"  intentional fallback:  {bands['intentional-fallback']:>6d} ({bands['intentional-fallback'] / unique:.1%})"
    )
    if gates["detection_recall_low"] or gates["detection_recall_medium"]:
        logger.warning(
            f"  gate-downgraded:       low={gates['detection_recall_low']:>4d}  "
            f"med={gates['detection_recall_medium']:>4d}  "
            f"(see report `gate` field)"
        )
    logger.info(f"  per-act report → {REPORT_PATH}")
    logger.success(f"transform: DONE — {unique} acts → stdout")


if __name__ == "__main__":
    main()
