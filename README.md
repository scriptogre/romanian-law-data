.# Romanian Law Data (Parquet)

Zstd-compressed Parquet exports of the Romanian legal corpus (acts, articles, paragraphs) for use with [DuckDB](https://duckdb.org/). Sourced from [legislatie.just.ro](https://legislatie.just.ro/) (Ministry of Justice) via its public SOAP API.

Automated daily via GitHub Actions. Download from [Releases](https://github.com/scriptogre/romanian-law-data/releases).

## Tables

| Table | Content | Rows |
|---|---|---|
| **acte** | One row per act (LEGE, OUG, HG, ORDIN, DECIZIE, …) with metadata + full text | ~187k |
| **articole** | One row per article (parsed from `acte.content`) | ~993k |
| **alineate** | One row per paragraph — the finest citation unit (e.g. `art. 188 alin. (1)`) | ~1.96M |

Tables use Romanian legal vocabulary (`acte`, `articole`, `alineate`); columns use English SQL convention (`type`, `published_at`, `gazette_number`, …) with Romanian `COMMENT ON` metadata in [`create_views.sql`](create_views.sql).

## Subject lenses

`create_views.sql` also exposes pre-filtered views over `acte` for each canonical code and for jurisprudence:

| View | Filters |
|---|---|
| `constitutie` | Constituția României (1991, republicată 2003) |
| `cod_civil` | Legea 287/2009 |
| `cod_penal` | Legea 286/2009 |
| `cod_muncii` | Legea 53/2003 (republicată) |
| `cod_procedura_civila` | Legea 134/2010 (republicată) |
| `cod_procedura_penala` | Legea 135/2010 |
| `cod_fiscal` | Legea 227/2015 |
| `jurisprudenta` | CCR + ÎCCJ decisions |

## Usage

```bash
# Download the latest bundle
gh release download -R scriptogre/romanian-law-data
tar xzf laws.tar.gz -C data/
```

```python
import duckdb
conn = duckdb.connect()
conn.execute(open("data/create_views.sql").read())
conn.execute("""
    SELECT full_path, content
    FROM articole
    WHERE act_id IN (SELECT id FROM cod_penal)
      AND number = 188
""").fetchall()
```

## Pipeline

```
collect.py    SOAP API → data/raw_acts.jsonl
normalize.py  → fix encoding, dedup, extract dates + gazette number
parse.py      → extract articles + alineate
export.py     → write parquet bundle + sha256
```

Stage outputs checkpoint as JSONL so the pipeline is resumable.

```bash
uv sync
uv run python -m scripts.collect
uv run python -m scripts.normalize
uv run python -m scripts.parse
uv run python -m scripts.export
```

## License

The corpus is published by the Romanian Ministry of Justice and is public information. This repository only provides format conversion + pipeline tooling.