# Romanian Law Data

Zstd Parquet of the Romanian legal corpus (documents, articles, paragraphs, relationships) for [DuckDB](https://duckdb.org/). From [legislatie.just.ro](https://legislatie.just.ro/) (Ministry of Justice), scraped via the public HTML detail pages.

Rebuilt daily by GitHub Actions. Download from [Releases](https://github.com/scriptogre/romanian-law-data/releases).

## Tables

| Table | Content | Rows |
|---|---|---|
| **documents** | One row per act (LEGE, OUG, HG, ORDIN, DECIZIE, ...) with metadata + full text | ~187k |
| **articles** | One row per article, parsed from `documents.content` | ~993k |
| **paragraphs** | One row per alineat, the finest citation unit (e.g. `art. 188 alin. (1)`) | ~1.96M |
| **relationships** | One row per directed edge (act → target): `repeals`, `amends`, `suspends`, ... | varies |

English column names (`type`, `document_number`, `published_at`, `gazette_number`, ...) with Romanian `COMMENT ON` metadata in [`create_views.sql`](create_views.sql).

## Subject lenses

`create_views.sql` pre-filters `documents` per canonical code and for case law:

| View | Filters |
|---|---|
| `constitution` | Constituția României (1991, republicată 2003) |
| `civil_code` | Legea 287/2009 |
| `penal_code` | Legea 286/2009 |
| `labor_code` | Legea 53/2003 (republicată) |
| `civil_procedure_code` | Legea 134/2010 (republicată) |
| `penal_procedure_code` | Legea 135/2010 |
| `tax_code` | Legea 227/2015 |
| `case_law` | CCR + ÎCCJ decisions |

## Usage

Download the latest release, then point DuckDB at the parquet files:

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

## Pipeline

```
etl.extract documents        sweep /Public/DetaliiDocument/{id} (resumable)
etl.extract relationships    fetch outgoing actiuni cache (resumable, sharded)
etl.transform                clean, parse, validate, write parquets, build FTS
```

`extract documents` and `extract relationships` checkpoint their progress into release-asset caches (`documents-cache`, `relationships`). The portal throttles per IP (~1 req/s sustained), so both run in CI as sharded matrices across many runners; each run resumes from the cache.

`transform` reads the document cache, runs the cleanup + parse pipeline, validates against Pandera schemas, builds the DuckDB FTS index, and writes the release parquets.

```bash
uv sync
uv run python -m etl.extract documents
uv run python -m etl.extract relationships
uv run python -m etl.transform
```

## License

Public corpus from the Romanian Ministry of Justice. This repo provides format conversion + pipeline tooling.
