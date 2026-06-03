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
DOCUMENTS = DATA_DIR / "documents.parquet"
ARTICLES = DATA_DIR / "articles.parquet"
PARAGRAPHS = DATA_DIR / "paragraphs.parquet"

# Smoke runs (`just smoke`) produce ~1k acts — too small for the invariant
# floors and code-specific article counts below. Skip whenever the parquet
# is obviously a partial slice rather than a full corpus build.
_FULL_CORPUS_FLOOR = 100_000


def _have_full_corpus() -> bool:
    if not (DOCUMENTS.exists() and ARTICLES.exists() and PARAGRAPHS.exists()):
        return False
    c = duckdb.connect(":memory:")
    try:
        (n,) = c.execute(f"SELECT count(*) FROM read_parquet('{DOCUMENTS}')").fetchone()
    finally:
        c.close()
    return n >= _FULL_CORPUS_FLOOR


pytestmark = pytest.mark.skipif(
    not _have_full_corpus(),
    reason="full corpus not built — run `just local` (or download release) first; smoke parquets too small",
)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(":memory:")
    c.execute(f"CREATE VIEW documents AS SELECT * FROM read_parquet('{DOCUMENTS}')")
    c.execute(f"CREATE VIEW articles AS SELECT * FROM read_parquet('{ARTICLES}')")
    c.execute(f"CREATE VIEW paragraphs AS SELECT * FROM read_parquet('{PARAGRAPHS}')")
    yield c
    c.close()


# ── volume floors ───────────────────────────────────────────────────────────
# These are LOWER bounds — the corpus grows over time. Refactor regressions
# show up as drops below these floors.


def test_documents_volume_floor(con):
    (n,) = con.execute("SELECT count(*) FROM documents").fetchone()
    assert n >= 180_000, f"documents dropped to {n}"


def test_articles_volume_floor(con):
    (n,) = con.execute("SELECT count(*) FROM articles").fetchone()
    assert n >= 950_000, f"articles dropped to {n}"


def test_paragraphs_volume_floor(con):
    (n,) = con.execute("SELECT count(*) FROM paragraphs").fetchone()
    assert n >= 1_900_000, f"paragraphs dropped to {n}"


# ── primary-key integrity ───────────────────────────────────────────────────


def test_no_duplicate_document_ids(con):
    (n,) = con.execute("SELECT count(*) - count(DISTINCT id) FROM documents").fetchone()
    assert n == 0


def test_no_duplicate_article_ids(con):
    (n,) = con.execute("SELECT count(*) - count(DISTINCT id) FROM articles").fetchone()
    assert n == 0


def test_no_duplicate_paragraph_ids(con):
    (n,) = con.execute("SELECT count(*) - count(DISTINCT id) FROM paragraphs").fetchone()
    assert n == 0


# ── referential integrity ───────────────────────────────────────────────────


def test_all_article_document_ids_exist(con):
    (n,) = con.execute(
        "SELECT count(*) FROM articles WHERE document_id NOT IN (SELECT id FROM documents)"
    ).fetchone()
    assert n == 0


def test_all_paragraphs_article_ids_exist(con):
    (n,) = con.execute(
        "SELECT count(*) FROM paragraphs WHERE article_id NOT IN (SELECT id FROM articles)"
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
        SELECT (SELECT count(*) FROM articles WHERE document_id = a.id) AS n_art
        FROM documents a
        WHERE {where_clause}
        ORDER BY length(content) DESC
        LIMIT 1
        """
    ).fetchall()
    # The famous codes occasionally vanish from a SOAP sweep (paging gaps,
    # republicări reshuffles). That's a CANARY, not a release blocker — Pandera
    # + FK integrity are the real gates. Skip instead of fail so the signal
    # shows up in the test report without nuking the run.
    if not rows:
        pytest.skip(f"{name}: not in this sweep (data canary)")
    n_art = rows[0][0]
    # Allow 5% tolerance — Romanian legal corpus has minor variations across
    # republicări. A drop below this means parser broke.
    assert n_art >= int(expected_article_count * 0.95), (
        f"{name}: got {n_art} articles, expected ~{expected_article_count}"
    )
