"""Unit tests against real SOAP samples in fixtures.jsonl.

Each fixture has `_id` (stable handle) + `_why` (description of edge case).
Tests target two layers:

    - Pure-helper functions (`*_str` in each transform module) — fast, easy
      to assert on specific strings.
    - LazyFrame transforms — verify Polars pipeline produces expected output
      on a small in-memory frame built from one fixture.

The orchestrator's end-to-end `transform_act(raw_dict)` exercises both.
"""

import polars as pl
import pytest

from etl.transform import transform_act
from etl.transforms import dates, dedup, issuers, text
from etl.transforms.parse import (
    extract_articles,
    extract_paragraphs,
    roman_to_int,
)


# ── text: pure helpers + Polars transforms ──────────────────────────────────


def test_fix_cedilla_str_translates_legacy_forms():
    assert text.fix_cedilla_str("activităţi şi instituţii") == "activități și instituții"
    assert text.fix_cedilla_str("ŢARA ŞI POPORUL") == "ȚARA ȘI POPORUL"


def test_fix_cedilla_str_handles_none():
    assert text.fix_cedilla_str(None) == ""


def test_fix_cedilla_lazyframe_applies_across_columns():
    lf = pl.LazyFrame(
        {
            "Titlu": ["activităţi şi"],
            "Text": ["text ţară"],
            "Emitent": ["Ministru"],
            "Publicatie": ["MO"],
        }
    )
    out = text.fix_cedilla(lf).collect()
    assert out["Titlu"][0] == "activități și"
    assert out["Text"][0] == "text țară"


def test_clean_titlu_strips_emitent_suffix():
    lf = pl.LazyFrame({"Titlu": ["LEGE nr. 1   EMITENT PARLAMENT\n PUBLICAT ÎN MO"]})
    out = text.clean_titlu(lf).collect()
    assert out["Titlu"][0] == "LEGE nr. 1"


def test_normalize_numar_zero_becomes_null():
    lf = pl.LazyFrame({"Numar": ["0", "287", "  0  ", "", None]})
    out = text.normalize_numar(lf).collect()
    assert out["Numar"].to_list() == [None, "287", None, None, None]


# ── issuers ─────────────────────────────────────────────────────────────────


def test_canonicalize_str_uppercases_preserving_diacritics():
    assert issuers.canonicalize_str("Curtea Constituțională") == "CURTEA CONSTITUȚIONALĂ"


def test_extract_emitent_str_recovers_from_text():
    text_body = "DECIZIA nr. 1 EMITENT CURTEA CONSTITUȚIONALĂ Publicat în..."
    assert issuers.extract_emitent_str(text_body, "Curtea Constitu?ională") == "CURTEA CONSTITUȚIONALĂ"


def test_extract_emitent_str_falls_back_when_no_text():
    assert issuers.extract_emitent_str(None, "GUVERNUL") == "GUVERNUL"


def test_extract_emitent_str_joint_issuers_separated_by_slash():
    text_body = "NORME EMITENT MINISTERUL SĂNĂTĂȚII MINISTERUL EDUCAȚIEI Nr. 1"
    out = issuers.extract_emitent_str(text_body, "")
    assert " / " in out
    assert "MINISTERUL SĂNĂTĂȚII" in out
    assert "MINISTERUL EDUCAȚIEI" in out


# ── dates ───────────────────────────────────────────────────────────────────


def test_extract_adopted_at_str_from_titlu():
    assert dates.extract_adopted_at_str("LEGE nr. 287 din 17 iulie 2009", "") == "2009-07-17"


def test_extract_adopted_at_str_falls_back_to_text():
    text_body = "ORDIN nr. 50 din 1 martie 2021\nbody"
    assert dates.extract_adopted_at_str("", text_body) == "2021-03-01"


def test_extract_adopted_at_str_returns_none_when_absent():
    assert dates.extract_adopted_at_str("title", "body") is None


def test_extract_gazette_str_handles_thousands_separator():
    iso, num = dates.extract_gazette_str("Publicat în MONITORUL OFICIAL nr. 1.216 din 5 ianuarie 2020")
    assert iso == "2020-01-05"
    assert num == 1216


def test_extract_gazette_str_no_match():
    assert dates.extract_gazette_str("no gazette here") == (None, None)


def test_extract_effective_at_str_preserves_far_future():
    """Year-6201 SOAP record exists in production. Phase 5 fix will clamp; for
    now, just lock pass-through."""
    assert dates.extract_effective_at_str("6201-06-01") == "6201-06-01"


def test_extract_effective_at_str_invalid():
    assert dates.extract_effective_at_str("not a date") is None
    assert dates.extract_effective_at_str(None) is None


# ── dedup ───────────────────────────────────────────────────────────────────


def test_dedup_drops_duplicate_titlu_emitent_pair():
    lf = pl.LazyFrame(
        {
            "Titlu": ["A", "A", "B", None, ""],
            "Emitent": ["X", "X", "Y", "Z", "Z"],
        }
    )
    out = dedup.by_titlu_emitent(lf).collect()
    assert out["Titlu"].to_list() == ["A", "B", ""]


# ── parse ───────────────────────────────────────────────────────────────────


def test_extract_articles_bis_variant():
    arts = extract_articles("Articolul 188 Main.\n\nArticolul 188 bis Added.\n")
    assert len(arts) == 2
    assert (arts[1]["number"], arts[1]["number_variant"]) == (188, "bis")


def test_extract_articles_caret_variant():
    arts = extract_articles("Articolul 1 First.\n\nArticolul 1^1 Inserted.\n")
    assert (arts[1]["number"], arts[1]["number_variant"]) == (1, "^1")


def test_extract_articles_roman():
    arts = extract_articles("Articolul I First.\n\nArticolul II Second.\n")
    assert [(a["number"], a["full_path"]) for a in arts] == [(1, "Art. I"), (2, "Art. II")]


def test_extract_articles_no_markers_returns_empty():
    assert extract_articles("No markers here.") == []


def test_extract_paragraphs_inline_numbered():
    paras = extract_paragraphs("Art. 1", "(1) Prima. (2) A doua.")
    assert [p["number"] for p in paras] == [1, 2]


def test_extract_paragraphs_no_markers_returns_one_null():
    paras = extract_paragraphs("Art. 1", "Single body.")
    assert len(paras) == 1 and paras[0]["number"] is None


def test_extract_paragraphs_ignores_alin_cross_references():
    paras = extract_paragraphs("Art. 1", "(1) Norma X aplică alin. (1) din alt act.")
    assert len([p for p in paras if p["number"] is not None]) == 1


def test_roman_to_int_compound():
    assert roman_to_int("MMXXIV") == 2024
    assert roman_to_int("CD") == 400
    assert roman_to_int("IX") == 9


# ── End-to-end via transform_act on real SOAP fixtures ─────────────────────


def test_cedilla_translated_end_to_end(raw_acts):
    out = transform_act(raw_acts["lege_cedilla_in_titlu"])
    assert "ţ" not in out["raw"]["Titlu"]
    assert "ş" not in out["raw"]["Titlu"]
    assert "Ordonanței de urgență" in out["raw"]["Titlu"]


def test_legacy_cedilla_in_text_translated(raw_acts):
    out = transform_act(raw_acts["decret_2005_legacy_text"])
    assert "Ş" not in out["raw"]["Text"]
    assert "Ș" in out["raw"]["Text"]


def test_question_mark_in_emitent_recovered(raw_acts):
    out = transform_act(raw_acts["decizie_ccr_question_mark"])
    assert out["raw"]["Emitent"] == "CURTEA CONSTITUȚIONALĂ"


def test_question_mark_in_emitent_recovered_for_courts(raw_acts):
    out = transform_act(raw_acts["sentinta_court_capital_T"])
    assert "?" not in out["raw"]["Emitent"]
    assert "IAȘI" in out["raw"]["Emitent"]


def test_emitent_uppercased_from_titlecase(raw_acts):
    out = transform_act(raw_acts["oug_caret_article_variant"])
    assert out["raw"]["Emitent"] == "GUVERNUL"


def test_emitent_diacritics_preserved_through_uppercasing(raw_acts):
    out = transform_act(raw_acts["sentinta_court_capital_T"])
    assert "Ș" in out["raw"]["Emitent"]
    assert "Ț" in out["raw"]["Emitent"]


def test_joint_issuers_separated_by_slash(raw_acts):
    out = transform_act(raw_acts["norma_joint_issuer"])
    parts = out["raw"]["Emitent"].split(" / ")
    assert len(parts) >= 2
    assert all("MINISTERUL" in p for p in parts)


def test_titlu_emitent_suffix_stripped(raw_acts):
    out = transform_act(raw_acts["lege_cedilla_in_titlu"])
    assert "EMITENT" not in out["raw"]["Titlu"]


def test_bom_stripped_from_titlu(raw_acts):
    out = transform_act(raw_acts["hotarare_camera_with_bom"])
    assert "﻿" not in out["raw"]["Titlu"]


def test_numar_zero_becomes_null(raw_acts):
    out = transform_act(raw_acts["raport_numar_zero"])
    assert out["raw"]["Numar"] is None


def test_real_numar_preserved(raw_acts):
    out = transform_act(raw_acts["lege_cedilla_in_titlu"])
    assert out["raw"]["Numar"] == "87"


def test_three_dates_extracted(raw_acts):
    out = transform_act(raw_acts["lege_cedilla_in_titlu"])["raw"]
    assert out["AdoptedAt"] == "2026-05-28"
    assert out["PublishedAt"] is not None
    assert out["EffectiveAt"] == "2026-05-31"
    assert out["GazetteNumber"] is not None


def test_gazette_parsed_from_text(raw_acts):
    out = transform_act(raw_acts["decret_articol_unic"])["raw"]
    assert out["PublishedAt"] == "2026-05-29"
    assert out["GazetteNumber"] == 457


def test_articol_unic_decret_parses_as_single_article(raw_acts):
    out = transform_act(raw_acts["decret_articol_unic"])
    assert len(out["articles"]) == 1
    assert out["articles"][0]["full_path"] == "Articol unic"
    assert out["articles"][0]["number"] is None


def test_narrative_comunicat_falls_back(raw_acts):
    out = transform_act(raw_acts["comunicat_narrative"])
    assert out["articles"][0]["full_path"] == "(unparsed)"
    assert out["quality"]["band"] == "intentional-fallback"


def test_transform_act_returns_full_shape(raw_acts):
    out = transform_act(raw_acts["oug_caret_article_variant"])
    assert set(out) == {"raw", "articles", "quality"}
    assert out["raw"]["Numar"] == "11"
    assert out["raw"]["Emitent"] == "GUVERNUL"


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
    """Every real fixture must complete cleanup + parse without raising.

    Catches edge cases that crash silently or partially. If a new pathological
    SOAP shape lands in the corpus, add it as a fixture and this test fails.
    """
    out = transform_act(raw_acts[fixture_id])
    assert out["raw"]["Titlu"]
    assert out["raw"]["Text"]
    assert len(out["articles"]) >= 1
    assert out["quality"]["band"] in ("high", "medium", "low", "intentional-fallback")
