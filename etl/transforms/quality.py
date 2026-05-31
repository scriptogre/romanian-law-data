"""
Per-act quality scoring.

Five signals weighted into a 0.0–1.0 composite, plus a detection-recall gate
that caps the band when articles were silently dropped:

    coverage              30%  sum(article.content) / len(text)
    detection_recall      25%  detected / expected article markers
    body_sanity           20%  fraction of articles with content ≥ 20 chars
    paragraph_contiguity  15%  fraction of multi-paragraph articles where
                               paragraph numbers form 1,2,3,... starting at 1
    no_tail_orphan        10%  1 - (chars after last article / total chars)

Bands:
    high quality          (≥0.85)
    medium quality        (0.5 – 0.85)
    low quality           (<0.5)
    intentional-fallback  no article markers AND none expected
"""

from etl.transforms.parse import (
    ANNEX_BOUNDARY_RE,
    RAW_MARKER_RE,
    SIGNING_BLOCK_RE,
    ZONE_START_RE,
)

TINY_BODY_THRESHOLD = 20

QUALITY_WEIGHTS = {
    "coverage": 0.30,
    "detection_recall": 0.25,
    "body_sanity": 0.20,
    "paragraph_contiguity": 0.15,
    "no_tail_orphan": 0.10,
}

HIGH_QUALITY = 0.85
MEDIUM_QUALITY = 0.50

# Detection-recall gate: catches silent article drops that the weighted
# composite would dilute.
DETECTION_RECALL_LOW_GATE = 0.50   # below → band = "low"
DETECTION_RECALL_MEDIUM_GATE = 0.85  # below → band capped at "medium"


def _structural_zone(text: str) -> tuple[int, int]:
    """(start, end) of the articulated portion — excludes preamble, annexes, signing."""
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


def _coverage(zone: tuple[int, int], articles: list[dict]) -> float:
    """Fraction of the articulated zone captured by article bodies."""
    captured = sum(len(a.get("content") or "") for a in articles)
    start, end = zone
    return min(1.0, captured / max(1, end - start))


def _detection_recall(expected_markers: int, articles: list[dict]) -> float:
    """detected_articles / expected_markers, capped at 1.0."""
    if expected_markers == 0:
        if articles and articles[0].get("full_path") == "Articol unic":
            return 1.0
        return 1.0 if not articles else 0.0
    return min(1.0, len(articles) / expected_markers)


def _body_sanity(articles: list[dict]) -> float:
    if not articles:
        return 0.0
    healthy = sum(1 for a in articles if len(a.get("content") or "") >= TINY_BODY_THRESHOLD)
    return healthy / len(articles)


def _paragraph_contiguity(articles: list[dict]) -> float:
    qualifying = contiguous = 0
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


def _no_tail_orphan(text: str, zone: tuple[int, int], articles: list[dict]) -> float:
    """1.0 minus (orphan chars / zone size). 1.0 = no orphan inside the zone."""
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
    start, zone_end = zone
    if last_end >= zone_end:
        return 1.0
    orphan_chars = zone_end - last_end
    return max(0.0, 1.0 - orphan_chars / max(1, zone_end - start))


def compute_quality(text: str, articles: list[dict], *, is_fallback: bool) -> dict:
    """Per-act quality signals + composite score + band + (optional) gate downgrade."""
    expected_markers = len(RAW_MARKER_RE.findall(text or ""))

    if is_fallback:
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

    zone = _structural_zone(text)
    signals = {
        "coverage": _coverage(zone, articles),
        "detection_recall": _detection_recall(expected_markers, articles),
        "body_sanity": _body_sanity(articles),
        "paragraph_contiguity": _paragraph_contiguity(articles),
        "no_tail_orphan": _no_tail_orphan(text, zone, articles),
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
