"""Unit tests for etl.load — citation builder.

Each branch of `build_act_citation` is exercised once: type-shorthand,
singleton (codes), HG vs Hotărârea (government vs other issuer), date
fallback, unknown-type fallback.
"""

from datetime import date

from etl.load import build_act_citation


def test_citation_lege_with_year_from_adopted_at():
    assert build_act_citation("LEGE", "287", date(2009, 7, 17), "PARLAMENTUL") == "Legea 287/2009"


def test_citation_oug():
    assert (
        build_act_citation("ORDONANȚĂ DE URGENȚĂ", "100", date(2024, 5, 1), "GUVERNUL")
        == "OUG 100/2024"
    )


def test_citation_hotarare_government_is_HG():
    assert build_act_citation("HOTĂRÂRE", "100", date(2020, 1, 1), "GUVERNUL") == "HG 100/2020"


def test_citation_hotarare_other_issuer_is_Hotararea():
    """CCR / ÎCCJ hotărâri are NOT government HG-uri — different short form."""
    assert (
        build_act_citation("HOTĂRÂRE", "50", date(2020, 1, 1), "ÎNALTA CURTE DE CASAȚIE ȘI JUSTIȚIE")
        == "Hotărârea 50/2020"
    )


def test_citation_slash_in_number_preserved():
    """Some act_number values already include the year suffix ('287/2009')."""
    assert build_act_citation("LEGE", "287/2009", date(2009, 7, 17), "PARLAMENTUL") == "Legea 287/2009"


def test_citation_codul_civil_singleton():
    """Codes are singletons — never carry act_number/year in the citation."""
    assert build_act_citation("CODUL CIVIL", None, date(2009, 7, 17), "PARLAMENTUL") == "Codul Civil"


def test_citation_no_number_falls_back_to_date():
    """Reports / norms with no number use 'short din DATE' form."""
    assert (
        build_act_citation("RAPORT", None, date(2024, 8, 13), "AUTORITATEA ELECTORALĂ PERMANENTĂ")
        == "Raportul din 2024-08-13"
    )


def test_citation_unknown_type_title_cased():
    """Types not in the shorthand table fall back to title-cased raw type."""
    assert build_act_citation("DISPOZIȚIE", "5", date(2020, 1, 1), "X") == "Dispoziție 5/2020"
