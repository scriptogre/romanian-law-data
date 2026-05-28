"""
Stage 5 — audit.py

Sanity-check the parquet bundle. Prints per-dimension verdicts so we can
tell quickly whether the corpus is shit or decent before wiring views.
"""

import json
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).parent.parent / "data"
ACTE = DATA_DIR / "acte.parquet"
ARTICOLE = DATA_DIR / "articole.parquet"
ALINEATE = DATA_DIR / "alineate.parquet"
REPORT = DATA_DIR / "parse_report.jsonl"


def section(title: str) -> None:
    print(f"\n\033[1;36m── {title} ─────────────────────────────────────\033[0m")


def query(con, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def main() -> None:
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW acte AS SELECT * FROM read_parquet('{ACTE}')")
    con.execute(f"CREATE VIEW articole AS SELECT * FROM read_parquet('{ARTICOLE}')")
    con.execute(f"CREATE VIEW alineate AS SELECT * FROM read_parquet('{ALINEATE}')")

    section("volume")
    for table in ("acte", "articole", "alineate"):
        (n,) = query(con, f"SELECT COUNT(*) FROM {table}")[0:1][0:1][0]
        print(f"  {table:10s} {n:>10,d}")

    section("act type distribution")
    rows = query(
        con,
        "SELECT type, COUNT(*) c FROM acte GROUP BY type ORDER BY c DESC LIMIT 20",
    )
    for t, c in rows:
        print(f"  {t or '<NULL>':30s} {c:>8,d}")

    section("year coverage")
    rows = query(
        con,
        """
        SELECT EXTRACT(YEAR FROM adopted_at) y, COUNT(*) c
        FROM acte WHERE adopted_at IS NOT NULL
        GROUP BY y ORDER BY y
        """,
    )
    print(f"  {len(rows)} years, {rows[0][0]:.0f}{rows[-1][0]:.0f}")
    print("  newest 5:")
    for y, c in rows[-5:]:
        print(f"    {y:.0f}  {c:>6,d}")
    print("  oldest 5:")
    for y, c in rows[:5]:
        print(f"    {y:.0f}  {c:>6,d}")

    section("nulls in acte")
    rows = query(
        con,
        """
        SELECT
            SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END)            AS no_type,
            SUM(CASE WHEN issuer IS NULL THEN 1 ELSE 0 END)          AS no_issuer,
            SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END)           AS no_title,
            SUM(CASE WHEN content IS NULL OR LENGTH(content)=0 THEN 1 ELSE 0 END) AS no_content,
            SUM(CASE WHEN adopted_at IS NULL THEN 1 ELSE 0 END)      AS no_adopted,
            SUM(CASE WHEN published_at IS NULL THEN 1 ELSE 0 END)    AS no_published,
            SUM(CASE WHEN effective_at IS NULL THEN 1 ELSE 0 END)    AS no_effective,
            SUM(CASE WHEN gazette_number IS NULL THEN 1 ELSE 0 END)  AS no_mo
        FROM acte
        """,
    )
    (no_type, no_issuer, no_title, no_content, no_adopted, no_published, no_effective, no_mo) = (
        rows[0]
    )
    (total,) = query(con, "SELECT COUNT(*) FROM acte")[0]
    for label, n in [
        ("type", no_type),
        ("issuer", no_issuer),
        ("title", no_title),
        ("content empty", no_content),
        ("adopted_at", no_adopted),
        ("published_at", no_published),
        ("effective_at", no_effective),
        ("gazette_number", no_mo),
    ]:
        pct = 100.0 * n / total
        print(f"  {label:18s} {n:>8,d}  ({pct:5.2f}%)")

    section("articole structural")
    rows = query(
        con,
        """
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN number IS NULL THEN 1 ELSE 0 END) no_number,
            SUM(CASE WHEN content IS NULL OR LENGTH(content)=0 THEN 1 ELSE 0 END) empty_content,
            AVG(LENGTH(content)) avg_len,
            MEDIAN(LENGTH(content)) med_len,
            MAX(LENGTH(content)) max_len
        FROM articole
        """,
    )
    total, no_num, empty, avg_len, med_len, max_len = rows[0]
    print(f"  total                {total:>8,d}")
    print(f"  number IS NULL       {no_num:>8,d}  ({100.0 * no_num / total:.2f}%)")
    print(f"  empty content        {empty:>8,d}  ({100.0 * empty / total:.2f}%)")
    print(f"  content len  avg {avg_len:>7,.0f}  median {med_len:>5,.0f}  max {max_len:>8,d}")

    section("alineate structural")
    rows = query(
        con,
        """
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN number IS NULL THEN 1 ELSE 0 END) whole_article_fallback,
            AVG(LENGTH(content)) avg_len,
            MEDIAN(LENGTH(content)) med_len,
            MAX(LENGTH(content)) max_len
        FROM alineate
        """,
    )
    total, no_num, avg_len, med_len, max_len = rows[0]
    print(f"  total                       {total:>10,d}")
    print(f"  no alineat (whole-article)  {no_num:>10,d}  ({100.0 * no_num / total:.2f}%)")
    print(f"  content len  avg {avg_len:>6,.0f}  median {med_len:>4,.0f}  max {max_len:>8,d}")

    section("articole per act")
    rows = query(
        con,
        """
        WITH counts AS (
            SELECT act_id, COUNT(*) c FROM articole GROUP BY act_id
        )
        SELECT
            COUNT(*)              n_acts_with_articles,
            AVG(c)                avg_articles,
            MEDIAN(c)             med_articles,
            MAX(c)                max_articles,
            QUANTILE_CONT(c, 0.9) p90,
            QUANTILE_CONT(c, 0.99) p99
        FROM counts
        """,
    )
    n_acts_w_arts, avg_a, med_a, max_a, p90, p99 = rows[0]
    print(f"  acts with ≥1 article  {n_acts_w_arts:>8,d}")
    print(f"  avg / median          {avg_a:>5.1f} / {med_a:.0f}")
    print(f"  p90 / p99 / max       {p90:>5.0f} / {p99:.0f} / {max_a}")

    section("named-corpus spot-checks (well-known codices)")
    # Codes are stored under consolidated TipAct ("CODUL X"), not as the
    # enacting LEGE entry. (Expected art counts as of latest republicări.)
    spots = [
        ("Constituție 1991", "type='CONSTITUȚIE' AND EXTRACT(YEAR FROM adopted_at)=1991", 156),
        (
            "Cod Civil (Legea 287/2009)",
            "type='CODUL CIVIL' AND EXTRACT(YEAR FROM adopted_at)=2009 AND title ILIKE '%republicat%'",
            2664,
        ),
        (
            "Cod Penal (Legea 286/2009)",
            "type='CODUL PENAL' AND EXTRACT(YEAR FROM adopted_at)=2009",
            446,
        ),
        (
            "Cod Muncii (Legea 53/2003)",
            "type='CODUL MUNCII' AND EXTRACT(YEAR FROM adopted_at)=2003 AND title ILIKE '%republicat%'",
            281,
        ),
        (
            "Cod Fiscal (Legea 227/2015)",
            "type='CODUL FISCAL' AND EXTRACT(YEAR FROM adopted_at)=2015",
            503,
        ),
        (
            "Cod proc. civilă (134/2010)",
            "type='CODUL DE PROCEDURĂ CIVILĂ' AND EXTRACT(YEAR FROM adopted_at)=2010 AND title ILIKE '%republicat%'",
            1133,
        ),
        (
            "Cod proc. penală (135/2010)",
            "type='CODUL DE PROCEDURĂ PENALĂ' AND EXTRACT(YEAR FROM adopted_at)=2010",
            603,
        ),
    ]
    for name, where, expected_arts in spots:
        rows = query(
            con,
            f"""
            SELECT a.id, a.title, LENGTH(a.content) AS clen,
                (SELECT COUNT(*) FROM articole WHERE act_id = a.id) AS n_art,
                (SELECT COUNT(*) FROM alineate p JOIN articole ar ON ar.id=p.article_id WHERE ar.act_id=a.id) AS n_alin
            FROM acte a
            WHERE {where}
            ORDER BY LENGTH(a.content) DESC
            LIMIT 1
            """,
        )
        if not rows:
            print(f"  ❌ {name:38s} NOT FOUND")
            continue
        _id, title, clen, n_art, n_alin = rows[0]
        ratio = n_art / expected_arts if expected_arts else 1.0
        verdict = "✅" if ratio >= 0.95 else "⚠️ " if ratio >= 0.5 else "❌"
        title_short = (title or "").strip()[:55]
        print(
            f"  {verdict} {name:38s} {n_art:>4d}/{expected_arts:<4d} art ({ratio:.0%})  "
            f"{n_alin:>5d} alin / {clen:>8,d} chars  {title_short}"
        )

    section("ÎCCJ / CCR / DECIZIE coverage")
    rows = query(
        con,
        """
        SELECT type, COUNT(*) c
        FROM acte
        WHERE type IN ('DECIZIE', 'HOTARARE', 'AVIZ')
           OR issuer ILIKE '%ÎCCJ%'
           OR issuer ILIKE '%CONSTITU%'
           OR issuer ILIKE '%CASATIE%'
        GROUP BY type ORDER BY c DESC LIMIT 10
        """,
    )
    for t, c in rows:
        print(f"  {t or '<NULL>':30s} {c:>8,d}")

    section("outliers (massive acts)")
    rows = query(
        con,
        """
        SELECT id, type, number, LENGTH(content) AS clen,
            (SELECT COUNT(*) FROM articole WHERE act_id=a.id) n_art,
            substr(title,1,80) ttl
        FROM acte a
        ORDER BY LENGTH(content) DESC
        LIMIT 5
        """,
    )
    for _id, t, num, clen, n_art, ttl in rows:
        print(f"  {t or '?':10s} {(num or '?'):14s} {clen:>9,d} chars  {n_art:>4d} art  {ttl}")

    section("zero-article acts (parser gave up)")
    rows = query(
        con,
        """
        WITH zero AS (
            SELECT a.id, a.type, a.number, LENGTH(a.content) clen
            FROM acte a
            LEFT JOIN articole ar ON ar.act_id=a.id
            WHERE ar.id IS NULL
        )
        SELECT type, COUNT(*) c, AVG(clen) avg_len
        FROM zero GROUP BY type ORDER BY c DESC LIMIT 10
        """,
    )
    for t, c, avg_len in rows:
        print(f"  {t or '<NULL>':30s} {c:>8,d}  avg content {avg_len:>8,.0f} chars")

    section("parse quality bands (from parse_report.jsonl)")
    bands = {"high": 0, "medium": 0, "low": 0, "intentional-fallback": 0}
    gated = {"detection_recall_low": 0, "detection_recall_medium": 0}
    n = 0
    low_recall_samples: list[dict] = []
    with REPORT.open() as f:
        for line in f:
            rec = json.loads(line)
            n += 1
            band = rec.get("band", "low")
            recall = (rec.get("signals") or {}).get("detection_recall")
            # Apply gate retroactively when parse_report predates parse.py's gate logic.
            if band in ("high", "medium") and recall is not None:
                if recall < 0.5:
                    band = "low"
                    gated["detection_recall_low"] += 1
                    if len(low_recall_samples) < 5:
                        low_recall_samples.append(rec)
                elif recall < 0.85 and band == "high":
                    band = "medium"
                    gated["detection_recall_medium"] += 1
            bands[band] = bands.get(band, 0) + 1
    for label in ("high", "medium", "low", "intentional-fallback"):
        c = bands.get(label, 0)
        print(f"  {label:22s} {c:>8,d}  ({100.0 * c / n:5.2f}%)")
    print()
    print("  gate downgrades (marker_recall):")
    print(f"    → low     (recall <0.50)  {gated['detection_recall_low']:>6,d}")
    print(f"    → medium  (recall <0.85)  {gated['detection_recall_medium']:>6,d}")

    if low_recall_samples:
        section("sample gate-downgraded acts (recall <0.50)")
        for rec in low_recall_samples:
            num = rec.get("number") or "?"
            typ = rec.get("type") or "?"
            ttl = (rec.get("title") or "")[:55]
            det = rec.get("detected_articles", 0)
            exp = rec.get("expected_markers", 0)
            recall = (rec.get("signals") or {}).get("detection_recall", 0.0)
            print(f"  {typ:25s} {num:<8s}  {det:>4d}/{exp:<4d} art  recall={recall:.2f}  {ttl}")


if __name__ == "__main__":
    main()
