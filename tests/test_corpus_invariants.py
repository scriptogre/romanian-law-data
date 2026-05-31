"""Regression tests against the built parquet corpus.

Runs against `data/*.parquet` when present (i.e. after `etl.load`); skips
otherwise so unit tests stay green on a fresh clone.

These tests lock in known facts about the corpus. They catch silent regressions
like "the parser drops 600 articles after a refactor" without needing a fresh
build of the pipeline.
"""

from pathlib import Path

import duckdb
import pytest

DATA_DIR = Path(__file__).parent.parent / "data"
ACTE = DATA_DIR / "acte.parquet"
ARTICOLE = DATA_DIR / "articole.parquet"
ALINEATE = DATA_DIR / "alineate.parquet"


pytestmark = pytest.mark.skipif(
    not (ACTE.exists() and ARTICOLE.exists() and ALINEATE.exists()),
    reason="parquet corpus not built yet — run `just local` (or download release) first",
)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(":memory:")
    c.execute(f"CREATE VIEW acte AS SELECT * FROM read_parquet('{ACTE}')")
    c.execute(f"CREATE VIEW articole AS SELECT * FROM read_parquet('{ARTICOLE}')")
    c.execute(f"CREATE VIEW alineate AS SELECT * FROM read_parquet('{ALINEATE}')")
    yield c
    c.close()


# ── volume floors ───────────────────────────────────────────────────────────
# These are LOWER bounds — the corpus grows over time. Refactor regressions
# show up as drops below these floors.


def test_acte_volume_floor(con):
    (n,) = con.execute("SELECT count(*) FROM acte").fetchone()
    assert n >= 180_000, f"acte dropped to {n}"


def test_articole_volume_floor(con):
    (n,) = con.execute("SELECT count(*) FROM articole").fetchone()
    assert n >= 950_000, f"articole dropped to {n}"


def test_alineate_volume_floor(con):
    (n,) = con.execute("SELECT count(*) FROM alineate").fetchone()
    assert n >= 1_900_000, f"alineate dropped to {n}"


# ── primary-key integrity ───────────────────────────────────────────────────


def test_no_duplicate_act_ids(con):
    (n,) = con.execute("SELECT count(*) - count(DISTINCT id) FROM acte").fetchone()
    assert n == 0


def test_no_duplicate_article_ids(con):
    (n,) = con.execute("SELECT count(*) - count(DISTINCT id) FROM articole").fetchone()
    assert n == 0


def test_no_duplicate_paragraph_ids(con):
    (n,) = con.execute("SELECT count(*) - count(DISTINCT id) FROM alineate").fetchone()
    assert n == 0


# ── referential integrity ───────────────────────────────────────────────────


def test_all_article_act_ids_exist(con):
    (n,) = con.execute(
        "SELECT count(*) FROM articole WHERE act_id NOT IN (SELECT id FROM acte)"
    ).fetchone()
    assert n == 0


def test_all_alineate_article_ids_exist(con):
    (n,) = con.execute(
        "SELECT count(*) FROM alineate WHERE article_id NOT IN (SELECT id FROM articole)"
    ).fetchone()
    assert n == 0


# ── named-corpus spot checks (Romanian codes) ───────────────────────────────
# Article counts from latest republicări. A drop here means the parser broke.


@pytest.mark.parametrize(
    "name,where_clause,expected_article_count",
    [
        (
            "Cod Civil (Legea 287/2009, republicat)",
            "type='CODUL CIVIL' AND EXTRACT(YEAR FROM adopted_at)=2009 AND title ILIKE '%republicat%'",
            2664,
        ),
        (
            "Cod Penal (Legea 286/2009)",
            "type='CODUL PENAL' AND EXTRACT(YEAR FROM adopted_at)=2009",
            446,
        ),
        (
            "Cod Muncii (Legea 53/2003, republicat)",
            "type='CODUL MUNCII' AND EXTRACT(YEAR FROM adopted_at)=2003 AND title ILIKE '%republicat%'",
            281,
        ),
        (
            "Cod Fiscal (Legea 227/2015)",
            "type='CODUL FISCAL' AND EXTRACT(YEAR FROM adopted_at)=2015",
            503,
        ),
        (
            "Cod proc. civilă (republicat)",
            "type='CODUL DE PROCEDURĂ CIVILĂ' AND EXTRACT(YEAR FROM adopted_at)=2010 AND title ILIKE '%republicat%'",
            1133,
        ),
        (
            "Cod proc. penală (135/2010)",
            "type='CODUL DE PROCEDURĂ PENALĂ' AND EXTRACT(YEAR FROM adopted_at)=2010",
            603,
        ),
        ("Constituție 1991", "type='CONSTITUȚIE' AND EXTRACT(YEAR FROM adopted_at)=1991", 156),
    ],
)
def test_known_code_article_counts(con, name, where_clause, expected_article_count):
    rows = con.execute(
        f"""
        SELECT (SELECT count(*) FROM articole WHERE act_id = a.id) AS n_art
        FROM acte a
        WHERE {where_clause}
        ORDER BY length(content) DESC
        LIMIT 1
        """
    ).fetchall()
    assert rows, f"{name}: no matching act found"
    n_art = rows[0][0]
    # Allow 5% tolerance — Romanian legal corpus has minor variations across
    # republicări. A drop below this means parser broke.
    assert n_art >= int(expected_article_count * 0.95), (
        f"{name}: got {n_art} articles, expected ~{expected_article_count}"
    )
