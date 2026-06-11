"""Unit tests against real SOAP samples in fixtures.jsonl.

Each fixture has `_id` (stable handle) + `_why` (description of edge case).
Tests target two layers:

    - Pure-helper functions (`*_str` in each transform module) — fast, easy
      to assert on specific strings.
    - LazyFrame transforms — verify Polars pipeline produces expected output
      on a small in-memory frame built from one fixture.

The orchestrator's end-to-end `transform_document(raw_dict)` exercises both.
"""

import polars as pl
import pytest

from etl.transform import transform_document
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
            "TipAct": ["CONDIŢII"],  # legacy cedilla in TipAct also gets fixed
        }
    )
    out = text.fix_cedilla(lf).collect()
    assert out["Titlu"][0] == "activități și"
    assert out["Text"][0] == "text țară"
    assert out["TipAct"][0] == "CONDIȚII"


def test_decode_html_entities_str_named_and_numeric():
    s = '&quot;Curtea&quot; &amp; Parlament&#160;1&apos;a'
    assert text.decode_html_entities_str(s) == '"Curtea" & Parlament 1\'a'


def test_decode_html_entities_str_handles_none_and_empty():
    assert text.decode_html_entities_str(None) is None
    assert text.decode_html_entities_str("") == ""


def test_decode_html_entities_lazyframe_does_not_double_decode():
    lf = pl.LazyFrame(
        {
            "Titlu": ["&amp;lt;"],
            "Text": ["A &amp; B &quot;c&quot; &#160;d"],
            "Emitent": [""],
            "Publicatie": [""],
            "TipAct": [""],
        }
    )
    out = text.decode_html_entities(lf).collect()
    # single-pass: &amp;lt; -> &lt; (not <)
    assert out["Titlu"][0] == "&lt;"
    assert out["Text"][0] == 'A & B "c"  d'


def test_clean_text_collapses_nbsp_with_regular_spaces():
    """`&#160;` decodes to NBSP; `clean_text` must collapse it with adjacent spaces."""
    lf = pl.LazyFrame({"Text": ["A    B"]})
    out = text.clean_text(lf).collect()
    assert out["Text"][0] == "A B"


def test_fix_replacement_chars_strips_ufffd():
    assert text.fix_replacement_chars_str("CURTEA �N A�") == "CURTEA N A"
    lf = pl.LazyFrame(
        {
            "Titlu": ["LEGE �"],
            "Text": ["a�b"],
            "Emitent": [""],
            "Publicatie": [""],
            "TipAct": [""],
        }
    )
    out = text.fix_replacement_chars(lf).collect()
    assert out["Titlu"][0] == "LEGE "
    assert out["Text"][0] == "ab"


def test_normalize_nfc_composes_decomposed_breve():
    # 'a' + U+0306 combining breve  →  'ă' (single codepoint)
    decomposed = "ă"
    assert len(decomposed) == 2
    out = text.normalize_nfc_str(decomposed)
    assert out == "ă"
    assert len(out) == 1


def test_normalize_nfc_lazyframe_across_columns():
    lf = pl.LazyFrame(
        {
            "Titlu": ["ă"],
            "Text": ["ţ"],  # t + combining cedilla → ţ (legacy)
            "Emitent": ["ş"],
            "Publicatie": [""],
            "TipAct": [""],
        }
    )
    out = text.normalize_nfc(lf).collect()
    assert out["Titlu"][0] == "ă"
    assert out["Text"][0] == "ţ"
    assert out["Emitent"][0] == "ş"


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


def test_apply_aliases_rewrites_known_variants_only():
    # Picks a real entry from the corpus-derived YAML — every diacritic-stripped
    # variant gets collapsed onto its canonical form.
    lf = pl.LazyFrame({"Emitent": ["ACT INTERNATIONAL", "PARLAMENTUL", None]})
    out = issuers.apply_aliases(lf).collect()
    assert out["Emitent"].to_list() == ["ACT INTERNAȚIONAL", "PARLAMENTUL", None]


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
    """The pure helper passes far-future through; `clamp_far_future` nulls it."""
    assert dates.extract_effective_at_str("6201-06-01") == "6201-06-01"


def test_extract_effective_at_str_invalid():
    assert dates.extract_effective_at_str("not a date") is None
    assert dates.extract_effective_at_str(None) is None


def test_clamp_far_future_nulls_year_past_threshold():
    lf = pl.LazyFrame(
        {
            "AdoptedAt": ["2020-01-01", "6201-06-01", None],
            "PublishedAt": ["6201-06-01", "2020-01-01", "2099-12-31"],
            "EffectiveAt": ["2100-01-01", "2099-01-01", None],
        }
    )
    out = dates.clamp_far_future(lf).collect()
    assert out["AdoptedAt"].to_list() == ["2020-01-01", None, None]
    assert out["PublishedAt"].to_list() == [None, "2020-01-01", "2099-12-31"]
    assert out["EffectiveAt"].to_list() == [None, "2099-01-01", None]


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


def test_extract_articles_skips_empty_body_between_adjacent_markers():
    text = "Articolul 1\nArticolul 2 Cuprinsul articolului doi.\nArticolul 3 Trei."
    out = extract_articles(text)
    # Art. 1 has no body; should be dropped silently.
    assert [a["number"] for a in out] == [2, 3]
    assert all(a["content"] for a in out)


def test_extract_articles_rejects_pathological_repetition():
    """A document with 10 'Art. 8' rows is not articulated — return [] so the
    caller falls back to a single (unparsed) row."""
    text = "\n".join(f"Articolul 8 Linie {i}." for i in range(10))
    assert extract_articles(text) == []


def test_unique_article_matches_all_case_variants():
    """`Articolul UNIC` (uppercase UNIC) is by far the most common single-article
    marker in the corpus (35k+ rows) — must be matched alongside the other cases."""
    for marker in (
        "Articolul unic",
        "Articolul UNIC",   # the one we were missing
        "ARTICOLUL UNIC",
        "Articol unic",
        "ARTICOL UNIC",
    ):
        text = f"Preamble.\n{marker} Body content of the unique article."
        out = extract_articles(text)
        assert len(out) == 1, f"failed on {marker!r}"
        assert out[0]["full_path"] == "Articol unic"
        assert "Body content" in out[0]["content"]


def test_extract_articles_keeps_variant_repeats():
    """Real laws can repeat numbers via bis/^1 variants — keep these."""
    text = (
        "Articolul 1 Body one.\n"
        "Articolul 1 bis Body bis.\n"
        "Articolul 2 Body two.\n"
        "Articolul 2 bis Body two bis.\n"
        "Articolul 3 Body three.\n"
    )
    out = extract_articles(text)
    assert len(out) == 5
    assert [a["number_variant"] for a in out] == [None, "bis", None, "bis", None]


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


# ── End-to-end via transform_document on real SOAP fixtures ─────────────────────


def test_cedilla_translated_end_to_end(raw_documents):
    out = transform_document(raw_documents["lege_cedilla_in_titlu"])
    assert "ţ" not in out["raw"]["Titlu"]
    assert "ş" not in out["raw"]["Titlu"]
    assert "Ordonanței de urgență" in out["raw"]["Titlu"]


def test_legacy_cedilla_in_text_translated(raw_documents):
    out = transform_document(raw_documents["decret_2005_legacy_text"])
    assert "Ş" not in out["raw"]["Text"]
    assert "Ș" in out["raw"]["Text"]


def test_question_mark_in_emitent_recovered(raw_documents):
    out = transform_document(raw_documents["decizie_ccr_question_mark"])
    assert out["raw"]["Emitent"] == "CURTEA CONSTITUȚIONALĂ"


def test_question_mark_in_emitent_recovered_for_courts(raw_documents):
    out = transform_document(raw_documents["sentinta_court_capital_T"])
    assert "?" not in out["raw"]["Emitent"]
    assert "IAȘI" in out["raw"]["Emitent"]


def test_emitent_uppercased_from_titlecase(raw_documents):
    out = transform_document(raw_documents["oug_caret_article_variant"])
    assert out["raw"]["Emitent"] == "GUVERNUL"


def test_emitent_diacritics_preserved_through_uppercasing(raw_documents):
    out = transform_document(raw_documents["sentinta_court_capital_T"])
    assert "Ș" in out["raw"]["Emitent"]
    assert "Ț" in out["raw"]["Emitent"]


def test_joint_issuers_separated_by_slash(raw_documents):
    out = transform_document(raw_documents["norma_joint_issuer"])
    parts = out["raw"]["Emitent"].split(" / ")
    assert len(parts) >= 2
    assert all("MINISTERUL" in p for p in parts)


def test_titlu_emitent_suffix_stripped(raw_documents):
    out = transform_document(raw_documents["lege_cedilla_in_titlu"])
    assert "EMITENT" not in out["raw"]["Titlu"]


def test_bom_stripped_from_titlu(raw_documents):
    out = transform_document(raw_documents["hotarare_camera_with_bom"])
    assert "﻿" not in out["raw"]["Titlu"]


def test_numar_zero_becomes_null(raw_documents):
    out = transform_document(raw_documents["raport_numar_zero"])
    assert out["raw"]["Numar"] is None


def test_real_numar_preserved(raw_documents):
    out = transform_document(raw_documents["lege_cedilla_in_titlu"])
    assert out["raw"]["Numar"] == "87"


def test_three_dates_extracted(raw_documents):
    out = transform_document(raw_documents["lege_cedilla_in_titlu"])["raw"]
    assert out["AdoptedAt"] == "2026-05-28"
    assert out["PublishedAt"] is not None
    assert out["EffectiveAt"] == "2026-05-31"
    assert out["GazetteNumber"] is not None


def test_gazette_parsed_from_text(raw_documents):
    out = transform_document(raw_documents["decret_articol_unic"])["raw"]
    assert out["PublishedAt"] == "2026-05-29"
    assert out["GazetteNumber"] == 457


def test_articol_unic_decret_parses_as_single_article(raw_documents):
    out = transform_document(raw_documents["decret_articol_unic"])
    assert len(out["articles"]) == 1
    assert out["articles"][0]["full_path"] == "Articol unic"
    assert out["articles"][0]["number"] is None


def test_narrative_comunicat_falls_back(raw_documents):
    out = transform_document(raw_documents["comunicat_narrative"])
    assert out["articles"][0]["full_path"] == "(unparsed)"
    assert out["quality"]["band"] == "intentional-fallback"


def test_transform_document_returns_full_shape(raw_documents):
    out = transform_document(raw_documents["oug_caret_article_variant"])
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
def test_every_fixture_round_trips_without_exception(raw_documents, fixture_id):
    """Every real fixture must complete cleanup + parse without raising.

    Catches edge cases that crash silently or partially. If a new pathological
    SOAP shape lands in the corpus, add it as a fixture and this test fails.
    """
    out = transform_document(raw_documents[fixture_id])
    assert out["raw"]["Titlu"]
    assert out["raw"]["Text"]
    assert len(out["articles"]) >= 1
    assert out["quality"]["band"] in ("high", "medium", "low", "intentional-fallback")
