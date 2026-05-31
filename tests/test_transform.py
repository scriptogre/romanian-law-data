"""Unit tests for etl.transform — runs against real SOAP samples in fixtures.jsonl.

Each fixture is annotated with `_id` (stable handle for tests) and `_why`
(human-readable description of the edge case it exercises). Adding a new test
case usually means: (a) add a row to fixtures.jsonl with a real SOAP response,
(b) write the assertion here referencing it by `_id`.
"""

import pytest

from etl.transform import (
    _extract_articles,
    _extract_paragraphs,
    extract_adopted_at,
    extract_effective_at,
    extract_gazette,
    normalize_act,
    parse_act,
    roman_to_int,
    transform_act,
)


# ── Top-level normalize behaviour against real SOAP responses ───────────────
#
# Each test below picks one fixture and asserts the property the fixture was
# selected for. The fixture's `_why` field documents the case in the JSONL.


def test_cedilla_in_titlu_translated_to_modern_forms(raw_acts):
    """lege_cedilla_in_titlu has 'Ordonanţei de urgenţă' (legacy ţ) in Titlu."""
    src = raw_acts["lege_cedilla_in_titlu"]
    assert "ţ" in src["Titlu"], "fixture sanity: source should have legacy cedilla"
    out = normalize_act(src)
    assert "ţ" not in out["Titlu"]
    assert "ş" not in out["Titlu"]
    assert "Ordonanței de urgență" in out["Titlu"]


def test_legacy_cedilla_in_text_emitent_block_translated(raw_acts):
    """decret_2005_legacy_text has 'PREŞEDINTELE' (legacy Ş) in Text EMITENT block."""
    src = raw_acts["decret_2005_legacy_text"]
    assert "Ş" in src["Text"], "fixture sanity: pre-2007 Text uses legacy capital Ş"
    out = normalize_act(src)
    # The Text passes through fix_cedilla, so the legacy form must be gone.
    assert "Ş" not in out["Text"]
    assert "Ș" in out["Text"]


def test_question_mark_in_emitent_recovered_from_text(raw_acts):
    """SOAP corrupts non-ASCII diacritics to '?'. Recovery reads the Text EMITENT block."""
    src = raw_acts["decizie_ccr_question_mark"]
    assert "?" in src["Emitent"]
    out = normalize_act(src)
    assert out["Emitent"] == "CURTEA CONSTITUȚIONALĂ"
    assert "?" not in out["Emitent"]


def test_question_mark_in_emitent_recovered_for_courts(raw_acts):
    """Same recovery for 'Curtea de Apel Ia?i' → 'CURTEA DE APEL IAȘI'."""
    src = raw_acts["sentinta_court_capital_T"]
    assert "?" in src["Emitent"]
    out = normalize_act(src)
    assert "?" not in out["Emitent"]
    assert "IAȘI" in out["Emitent"]


def test_emitent_uppercased_when_soap_returns_titlecase(raw_acts):
    """OUG SOAP returns Emitent='Guvernul'; canonicalized to GUVERNUL."""
    src = raw_acts["oug_caret_article_variant"]
    assert src["Emitent"] == "Guvernul"
    out = normalize_act(src)
    assert out["Emitent"] == "GUVERNUL"


def test_emitent_diacritics_preserved_through_uppercasing(raw_acts):
    """Romanian Ș / Ț must survive .upper() (Python normally handles this but
    we depend on it — locking the property in)."""
    out = normalize_act(raw_acts["sentinta_court_capital_T"])
    assert "Ș" in out["Emitent"]  # IAȘI
    assert "Ț" in out["Emitent"]  # SECȚIA


def test_joint_issuers_separated_by_slash(raw_acts):
    """norma_joint_issuer has three back-to-back MINISTERUL prefixes in Text."""
    src = raw_acts["norma_joint_issuer"]
    out = normalize_act(src)
    parts = out["Emitent"].split(" / ")
    assert len(parts) >= 2
    assert all("MINISTERUL" in p for p in parts)


def test_titlu_emitent_suffix_stripped(raw_acts):
    """SOAP appends 'EMITENT ... PUBLICAT ÎN ...' to Titlu — we strip it."""
    src = raw_acts["lege_cedilla_in_titlu"]
    assert "EMITENT" in src["Titlu"], "fixture sanity"
    out = normalize_act(src)
    assert "EMITENT" not in out["Titlu"]


def test_bom_stripped_from_titlu(raw_acts):
    """hotarare_camera_with_bom has \\ufeff at the start of Titlu."""
    src = raw_acts["hotarare_camera_with_bom"]
    assert src["Titlu"].startswith("﻿") or "﻿" in src["Titlu"][:5]
    out = normalize_act(src)
    assert "﻿" not in out["Titlu"]


def test_numar_zero_placeholder_becomes_null(raw_acts):
    """SOAP returns '0' when no act number was assigned (e.g. for RAPORT)."""
    src = raw_acts["raport_numar_zero"]
    assert src["Numar"] == "0"
    out = normalize_act(src)
    assert out["Numar"] is None


def test_real_numar_preserved(raw_acts):
    """A normal numeric act number stays a string."""
    out = normalize_act(raw_acts["lege_cedilla_in_titlu"])
    assert out["Numar"] == "87"


def test_three_dates_extracted_from_real_act(raw_acts):
    """The most basic invariant — every well-formed act yields all three dates."""
    out = normalize_act(raw_acts["lege_cedilla_in_titlu"])
    assert out["AdoptedAt"] == "2026-05-28"          # from Titlu "din 28 mai 2026"
    assert out["PublishedAt"] is not None             # from MONITORUL OFICIAL nr. ...
    assert out["EffectiveAt"] == "2026-05-31"         # from SOAP DataVigoare
    assert out["GazetteNumber"] is not None


def test_gazette_number_parsed_from_text(raw_acts):
    """The MO publication phrase yields both ISO date and integer issue."""
    out = normalize_act(raw_acts["decret_articol_unic"])
    assert out["PublishedAt"] == "2026-05-29"
    assert out["GazetteNumber"] == 457


# ── extract_adopted_at: source priority (Titlu first, then Text fallback) ──


def test_extract_adopted_at_falls_back_to_text():
    text = "ORDIN nr. 50 din 1 martie 2021\nMore body text\n"
    assert extract_adopted_at("", text) == "2021-03-01"


def test_extract_adopted_at_returns_none_when_absent():
    assert extract_adopted_at("title without date", "body without date") is None


# ── extract_gazette: thousands separator + no-match ────────────────────────


def test_extract_gazette_handles_thousands_separator():
    """Romanian convention writes 'nr. 1.216' for issue #1216."""
    iso, num = extract_gazette("Publicat în MONITORUL OFICIAL nr. 1.216 din 5 ianuarie 2020")
    assert iso == "2020-01-05"
    assert num == 1216


def test_extract_gazette_no_match():
    assert extract_gazette("no gazette phrase here") == (None, None)


# ── extract_effective_at: preserves bogus dates (Phase 5 will clamp) ───────


def test_extract_effective_at_preserves_far_future_date():
    """The year-6201 SOAP record exists in production. Lock current pass-through
    behaviour; Phase 5 introduces a sane clamp + null."""
    assert extract_effective_at("6201-06-01") == "6201-06-01"


def test_extract_effective_at_invalid():
    assert extract_effective_at("not a date") is None
    assert extract_effective_at(None) is None


# ── roman_to_int: just the compound case (function is 9 lines) ─────────────


def test_roman_to_int_compound():
    assert roman_to_int("MMXXIV") == 2024
    assert roman_to_int("CD") == 400
    assert roman_to_int("IX") == 9


# ── Article extraction on synthetic shapes ─────────────────────────────────
# These use minimal hand-crafted strings because they probe specific regex
# behaviour (variants, fallback to "Articol unic"). Real fixtures cover
# end-to-end parse via test_parse_act_* below.


def test_extract_articles_bis_variant():
    arts = _extract_articles("Articolul 188 Main.\n\nArticolul 188 bis Added later.\n")
    assert len(arts) == 2
    assert arts[1]["number"] == 188
    assert arts[1]["number_variant"] == "bis"
    assert arts[1]["full_path"] == "Art. 188 bis"


def test_extract_articles_caret_variant():
    """Real OUG uses 'Articolul 1^1' for inserted articles."""
    arts = _extract_articles("Articolul 1 First.\n\nArticolul 1^1 Inserted.\n")
    assert len(arts) == 2
    assert arts[1]["number"] == 1
    assert arts[1]["number_variant"] == "^1"
    assert arts[1]["full_path"] == "Art. 1^1"


def test_extract_articles_roman():
    arts = _extract_articles("Articolul I First step.\n\nArticolul II Second step.\n")
    assert [(a["number"], a["full_path"]) for a in arts] == [(1, "Art. I"), (2, "Art. II")]


def test_extract_articles_no_markers_returns_empty():
    assert _extract_articles("No article markers here at all.") == []


# ── Paragraph extraction ────────────────────────────────────────────────────


def test_extract_paragraphs_inline_numbered():
    paras = _extract_paragraphs("Art. 1", "(1) Prima dispoziție. (2) A doua dispoziție.")
    nums = [p["number"] for p in paras]
    assert nums == [1, 2]
    assert paras[0]["full_path"] == "Art. 1 alin. (1)"


def test_extract_paragraphs_no_markers_returns_one_null():
    paras = _extract_paragraphs("Art. 1", "Monolithic article body.")
    assert len(paras) == 1
    assert paras[0]["number"] is None
    assert paras[0]["full_path"] == "Art. 1"


def test_extract_paragraphs_ignores_alin_cross_references():
    """'alin. (1)' inside body text refers to another article; not a paragraph marker."""
    paras = _extract_paragraphs("Art. 1", "(1) Norma X aplică alin. (1) din alt act.")
    assert len([p for p in paras if p["number"] is not None]) == 1


# ── End-to-end transform_act on real fixtures ──────────────────────────────


def test_articol_unic_decret_parses_as_single_article(raw_acts):
    parsed = transform_act(raw_acts["decret_articol_unic"])
    assert len(parsed["articles"]) == 1
    assert parsed["articles"][0]["full_path"] == "Articol unic"
    assert parsed["articles"][0]["number"] is None


def test_narrative_comunicat_falls_back_to_unparsed(raw_acts):
    parsed = transform_act(raw_acts["comunicat_narrative"])
    assert parsed["articles"][0]["full_path"] == "(unparsed)"
    # No article markers in source AND no parse attempt → intentional fallback.
    assert parsed["quality"]["band"] == "intentional-fallback"


def test_transform_act_returns_full_shape(raw_acts):
    """raw + articles + quality keys; raw stays a dict with normalized fields."""
    parsed = transform_act(raw_acts["oug_caret_article_variant"])
    assert set(parsed) == {"raw", "articles", "quality"}
    assert parsed["raw"]["Numar"] == "11"
    assert parsed["raw"]["Emitent"] == "GUVERNUL"
    assert parsed["quality"]["band"] in ("high", "medium", "low", "intentional-fallback")


@pytest.mark.parametrize(
    "fixture_id",
    [
        "decret_articol_unic",
        "lege_cedilla_in_titlu",
        "decizie_ccr_question_mark",
        "oug_caret_article_variant",
        "hotarare_camera_with_bom",
        "raport_numar_zero",
        "cuantum_total_boilerplate",
        "norma_joint_issuer",
        "schema_html_entities",
        "sentinta_court_capital_T",
        "decret_2005_legacy_text",
        "comunicat_narrative",
    ],
)
def test_every_fixture_round_trips_without_exception(raw_acts, fixture_id):
    """Every real fixture must complete normalize + parse without raising.

    Catches edge cases that crash silently or partially. If a new pathological
    SOAP shape lands in the corpus, add it as a fixture and this test fails.
    """
    parsed = transform_act(raw_acts[fixture_id])
    # Minimum guarantees: normalized act has a non-null Titlu and Text;
    # at least one article exists (real or fallback).
    assert parsed["raw"]["Titlu"]
    assert parsed["raw"]["Text"]
    assert len(parsed["articles"]) >= 1
    assert parsed["quality"]["band"] in (
        "high", "medium", "low", "intentional-fallback",
    )
