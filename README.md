# Romanian Law Data

Romanian legal corpus (documents, articles, paragraphs, relationships) as zstd parquets for [DuckDB](https://duckdb.org/). 

From [legislatie.just.ro](https://legislatie.just.ro/) (Ministry of Justice), scraped via the public HTML.

Rebuilt daily by GitHub Actions. Download from [Releases](https://github.com/scriptogre/romanian-law-data/releases).

## Tables

| Table | Content | Rows |
|---|---|---|
| **documents** | One row per act (LEGE, OUG, HG, ORDIN, DECIZIE, ...) with metadata + full text | ~187k |
| **articles** | One row per article, parsed from `documents.content` | ~993k |
| **paragraphs** | One row per alineat, the finest citation unit (e.g. `art. 188 alin. (1)`) | ~1.96M |
| **relationships** | One row per directed edge (act → target): `repeals`, `amends`, `suspends`, ... | varies |

English column names (`type`, `document_number`, `published_at`, `gazette_number`, ...) with Romanian `COMMENT ON` metadata in [`create_views.sql`](create_views.sql).

## Usage

Download latest release, and open it using DuckDB:

```bash
mkdir -p data
gh release download -R scriptogre/romanian-law-data --pattern '*.parquet' --pattern 'create_views.sql' --dir data
```

```python
import duckdb
conn = duckdb.connect()
conn.execute(open("data/create_views.sql").read())
conn.execute("""
    SELECT document_citation, link, article_citation, content
    FROM articles
    WHERE document_id IN (SELECT id FROM penal_code)
      AND article_number = 188
""").fetchall()
```
