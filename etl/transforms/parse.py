"""
Article + paragraph extraction.

Operates one act at a time on the cleaned `Text`. Polars's Rust regex
doesn't support lookbehind/lookahead so this stage stays pure Python and
is called row-by-row after the Polars cleanup pipeline collects.
"""

import re

# A "raw article marker" — count-only regex used to compute detection_recall.
# Looser than ARTICLE_RE on purpose: we want to know how many candidate
# markers the text CONTAINS, even if some weren't extracted.
RAW_MARKER_RE = re.compile(
    r"(?:^|\n)[ \t]*(?:Articolul|ARTICOLUL|Art\.|ART\.)[ \t]+(?:\d+|[IVXLCDM]+\b)",
)

# Annex header. Real annexes are detected by article-number continuity (see
# `_find_real_annex`) because `Anexa nr. 1` also appears inline as a cross-
# reference inside article bodies.
ANNEX_BOUNDARY_RE = re.compile(
    r"(?:^|\n)\s*(?:ANEX[ĂA]|Anex[ăa])"
    r"(?:\s+(?:NR\.|nr\.|\d|[IVX]+|la)|\s*\Z)",
)

# Signing block — appears at the end of most Romanian acts. Excluded from
# coverage / orphan-tail calculations.
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

# Any recognized marker — numbered articles OR "Articol unic".
ZONE_START_RE = re.compile(
    r"(?:^|\n)[ \t]*("
    r"Articolul|ARTICOLUL|Art\.|ART\."
    r"|Articol\s+unic|ARTICOL\s+UNIC|Articolul\s+unic"
    r")\b",
)

# DECRETs and other short acts often use a single-article marker. The corpus
# uses every case combination — `Articolul unic`, `Articolul UNIC`, `Articol
# unic`, `ARTICOL UNIC`. Case-insensitive matching captures all of them.
# Treated as a single article with number=NULL.
UNIQUE_ARTICLE_RE = re.compile(
    r"(?:^|\n)[ \t]*Articol(?:ul)?\s+unic\b[ \t]*",
    re.IGNORECASE,
)

# Article marker (NOT body). Body is the slice between successive matches.
# Case-sensitive on the keyword to avoid matching "art. 188" cross-references.
# The trailing lookahead `(?=[ \t]|$)` requires whitespace after the number;
# real SOAP text always has it (article title is inline with the marker).
#
# Known limitation: dotted hierarchical numbering ("Articolul 1.1.1") used by
# technical regulations isn't matched. Affects ~0.02% of acts; they fall back
# to "(unparsed)".
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

# Paragraphs: "(1)", "(2)" as paragraph markers.
# Disambiguates from "alin. (1) se aplică" cross-references by requiring
# the marker to be preceded by whitespace/punctuation and followed by an
# uppercase Romanian letter.
PARAGRAPH_RE = re.compile(
    r"""
        (?:^|(?<=[\s.;,!?]))
        \((?P<number>\d+)\)
        [ \t]+
        (?=[A-ZĂÂÎȘȚ])
    """,
    re.VERBOSE,
)


# ── Helpers ─────────────────────────────────────────────────────────────────

ROMAN_VALUES = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}

# Below this distinct/total (number, variant) ratio, an extraction is treated
# as pathological — a non-narrative document (TABLOU DE EVIDENȚĂ, list/table
# documents) whose rows happen to start with "Art. N". Real laws fall well
# above 0.25 even when articles repeat numbers with `bis` / `^N` variants.
_PATHOLOGICAL_DISTINCT_RATIO = 0.25
_PATHOLOGICAL_MIN_ARTICLES = 5


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
    """Numeric article number (strips Romanian thousands separator)."""
    raw = match.group("number")
    return int(raw.replace(".", "")) if raw else None


def _find_real_annex(text: str, all_matches: list[re.Match]) -> int:
    """First real-annex position, or len(text).

    ANNEX_BOUNDARY_RE matches inline body cross-references too. Real annexes
    are detected by article-number continuity: if any number AFTER a candidate
    is a continuation (> the running max BEFORE), the candidate is an inline
    reference. Otherwise it's a real annex (numbers restart, or no articles
    follow).
    """
    candidates = list(ANNEX_BOUNDARY_RE.finditer(text))
    if not candidates:
        return len(text)
    if not all_matches:
        return candidates[0].start()

    for candidate in candidates:
        pos = candidate.start()
        before = [n for m in all_matches if m.start() < pos and (n := _article_number(m)) is not None]
        after = [n for m in all_matches if m.start() > pos and (n := _article_number(m)) is not None]
        if not after:
            return pos
        if before and any(n > max(before) for n in after):
            continue
        return pos
    return len(text)


# ── Article extraction ──────────────────────────────────────────────────────


def extract_articles(text: str) -> list[dict]:
    """Slice plain text into articles. Annex region excluded so template
    articles inside annexes don't pollute the parent act's numbering."""
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

        if not content:
            # Empty body — two adjacent markers with no slice between them.
            # Real articles always have some text; skipping these drops the
            # 705 empty-articole / 705 empty-alineate rows the audit found.
            continue

        articles.append(
            {
                "number": number,
                "number_variant": variant,
                "full_path": _format_article_path(number, variant, roman=roman),
                "content": content,
            }
        )

    # Reject pathological extractions (e.g. TABLOU DE EVIDENȚĂ act 141153
    # producing 420 "Art. 8" rows from a non-narrative table). The caller
    # treats `[]` as "no articles" and falls back to a single (unparsed) row.
    if len(articles) >= _PATHOLOGICAL_MIN_ARTICLES:
        keys = {(a["number"], a["number_variant"]) for a in articles}
        if len(keys) / len(articles) < _PATHOLOGICAL_DISTINCT_RATIO:
            return []

    return articles


# ── Paragraph extraction ────────────────────────────────────────────────────


def extract_paragraphs(article_path: str, content: str) -> list[dict]:
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
    # Capture any prologue text before the first (1) marker.
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
