"""
Stage 3 — parse.py

Extract articles + paragraphs (alineate) from each act's plain text.

Reads normalized acts from stdin (one JSONL line per act, as emitted by
normalize.py) and writes parsed acts to stdout for export.py to pipe-consume.
Both intermediate files (`normalized_acts.jsonl` and `parsed.jsonl`) stay off
disk. The per-act diagnostic report still writes to `data/parse_report.jsonl`.

Output shape (per stdout line):

    {
        "raw": <normalized act dict>,
        "articles": [
            {
                "number": 188,                         # INTEGER
                "number_variant": null | "bis" | ...,  # VARCHAR NULL
                "full_path": "Art. 188",
                "content": "...",                       # full article text
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
        ]
    }

Quality framework (per-act, 0.0 – 1.0):
    coverage              30%  sum(article.content) / len(text)
    detection_recall      25%  detected / expected article markers
    body_sanity           20%  fraction of articles with content ≥ 20 chars
    paragraph_contiguity  15%  fraction of multi-paragraph articles where
                               paragraph numbers form 1,2,3,... starting at 1
    no_tail_orphan        10%  1 - (chars after last article / total chars)

Bands reported at end of run:
    high quality   (≥0.85)
    medium quality (0.5 – 0.85)
    low quality    (<0.5)
    intentional fallback   — no article markers AND not expected to have any

Per-act scores written to `data/parse_report.jsonl`.
"""

import json
import re
import sys
from pathlib import Path

from loguru import logger

REPORT_PATH = Path(__file__).parent.parent / "data" / "parse_report.jsonl"

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

# ── Regex patterns ───────────────────────────────────────────────────────────
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


# ── Article extraction ───────────────────────────────────────────────────────

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


# ── Paragraph extraction ─────────────────────────────────────────────────────


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


# ── Quality scoring ──────────────────────────────────────────────────────────


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


# ── Driver ───────────────────────────────────────────────────────────────────


def parse_act(act: dict) -> dict:
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


def main() -> None:
    logger.info("parse: start (input=stdin)")
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    bands: dict[str, int] = {"high": 0, "medium": 0, "low": 0, "intentional-fallback": 0}
    gates: dict[str, int] = {"detection_recall_low": 0, "detection_recall_medium": 0}
    score_sum = 0.0

    with REPORT_PATH.open("w", encoding="utf-8") as report:
        for line in sys.stdin:
            if total and total % 10_000 == 0:
                avg = score_sum / total
                logger.info(
                    f"parse: progress  acts={total:>7d}  "
                    f"high={bands['high']:>5d}  med={bands['medium']:>4d}  "
                    f"low={bands['low']:>4d}  fallback={bands['intentional-fallback']:>5d}  "
                    f"mean_score={avg:.3f}"
                )
            act = json.loads(line)
            parsed = parse_act(act)
            sys.stdout.write(json.dumps(parsed, ensure_ascii=False) + "\n")
            total += 1

            quality = parsed["quality"]
            bands[quality["band"]] += 1
            score_sum += quality["score"]
            if quality.get("gate"):
                gates[quality["gate"]] += 1

            report.write(
                json.dumps(
                    {
                        "title": act.get("Titlu"),
                        "type": act.get("TipAct"),
                        "number": act.get("Numar"),
                        "text_length": len(act.get("Text") or ""),
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

    avg = score_sum / total if total else 0.0
    logger.success(f"parse: DONE — {total} acts → stdout")
    logger.info(f"  mean quality score:    {avg:.3f}")
    logger.info(
        f"  high   (≥{HIGH_QUALITY}):           {bands['high']:>6d} ({bands['high'] / total:.1%})"
    )
    logger.info(
        f"  medium ({MEDIUM_QUALITY}–{HIGH_QUALITY}):       {bands['medium']:>6d} ({bands['medium'] / total:.1%})"
    )
    logger.info(
        f"  low    (<{MEDIUM_QUALITY}):           {bands['low']:>6d} ({bands['low'] / total:.1%})"
    )
    logger.info(
        f"  intentional fallback:  {bands['intentional-fallback']:>6d} ({bands['intentional-fallback'] / total:.1%})"
    )
    if gates["detection_recall_low"] or gates["detection_recall_medium"]:
        logger.warning(
            f"  gate-downgraded:       low={gates['detection_recall_low']:>4d}  "
            f"med={gates['detection_recall_medium']:>4d}  "
            f"(see report `gate` field)"
        )
    logger.info(f"  per-act report → {REPORT_PATH}")


if __name__ == "__main__":
    main()
