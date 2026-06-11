# Briefing: designing the version-aware data model

Context for a fresh session whose job is to explore the domain and the raw data, then
propose a normalized data model that captures full version history. This is exploratory.
Investigate, advise, challenge assumptions. Nothing here is settled except the principles.

## The goal

`romanian-law-data` scrapes the Romanian legal portal (legislatie.just.ro) into a
published corpus. The current model keeps **one row per act** and throws away every
historical version. We want the opposite: **keep everything**, and be able to answer
"what did this law say on date X."

Settle on a data model that does that. Don't write pipeline code yet; get the model right first.

## The architecture (the user's framing)

Three layers:

| Layer | Holds | Form |
|---|---|---|
| L1 raw | source responses, verbatim | `raw_*.parquet` (already exist) |
| L2 core | every fact, normalized, lossless, flexible | parquet tables (what we're designing) |
| L3 views | convenience plus temporal **state**, derived | SQL views (`create_views.sql`) |

Hard rule: **L2 stores facts, not state.** Store `valid_from` (a fact from the source);
do NOT store "is this in force / is this current". Compute that in L3 from the facts.
Point-in-time ("content on date X") is an L3 view over L2's validity windows.

Target conceptual model: **FRBR / Akoma Ntoso** for legislation.
- **Work**: the abstract act ("Legea 287/2009, Codul Civil"), stable across amendments.
- **Expression**: one dated consolidation/version of the work.
- **Manifestation**: one scraped portal page (`DetaliiDocument/{id}`).

The open problem is mapping the messy raw data onto this cleanly. That's the work.

## The source (legislatie.just.ro), reverse-engineered

- `GET /Public/DetaliiDocument/{id}` returns one page. Fields: `Titlu, Text, TipAct, Numar,
  Emitent, Publicatie, DataVigoare, LinkHtml`. `LinkHtml` ends in the portal id.
  IDs are swept across ~1..max to enumerate (the SOAP search API is unreliable).
- Each act page has an **`istoric_fa`** block: one dated link per consolidation
  ("Consolidarea din DD.MM.YYYY"), newest first. **Past forms link to a snapshot id; the
  current form has no link.** This is the version timeline. Captured in `raw_versions`.
- Relationship endpoints (`actiuniInduse` etc.) give directed edges (act A amends/repeals
  B). Captured in `raw_relationships`. Targets can point at snapshot ids (see id-sprawl below).

## The data you have (download from the `documents-cache` and `relationships` releases)

```bash
gh release download documents-cache -R scriptogre/romanian-law-data \
  --pattern 'raw_documents.parquet' --pattern 'raw_versions.parquet' --dir /tmp
gh release download relationships -R scriptogre/romanian-law-data \
  --pattern 'raw_relationships.parquet' --dir /tmp
```
Query with `uv run python` plus polars/duckdb. Read column metadata before materializing
`Text`; it is 8.6 GB uncompressed.

- **`raw_documents.parquet`**: one row per scraped page (manifestation).
  - 302,077 rows = 302,077 distinct portal ids. `Text` is 8.6 GB uncompressed.
  - Columns: `Titlu, Text, TipAct, Numar, Emitent, Publicatie, DataVigoare, LinkHtml`.
- **`raw_versions.parquet`**: schema `(document_id, date, version_id)`, dates `DD.MM.YYYY`.
  - 769,055 rows; 72,135 distinct `document_id`; 72,210 distinct `version_id`.
  - 71,807 of those `version_id` snapshots already have a page in `raw_documents`; only 403 don't.
  - Versions per act: median 3, mean 10.7, max 206.
- **`raw_relationships.parquet`**: directed edges parsed from `actiuniInduse` (outgoing only).

## What's already been found (verify, don't trust blindly)

1. **The current dedup destroys version history.** `etl/transforms/dedup.py` keeps the
   first row per `(Titlu, Emitent)`, dropping 50,014 of 302,077 rows. Those are NOT
   duplicates; every one is a distinct portal page. **0 dropped groups differ by act
   number;** ~49k share the same number plus in-force date (consolidation forms of one act),
   ~772 differ only by consolidation date. So they're versions, not different acts.
2. **Title is not a stable work key.** ~21k snapshot forms carry a *different* title than
   their own act, so they currently leak into `documents` as if they were separate acts,
   while ~50k get dropped. Title+issuer is the wrong identity.
3. **`raw_versions` is denser than expected and tangled.** 769k rows but only ~72k
   distinct snapshots, so the istoric appears to repeat (every page of an act lists the full
   timeline). And **71,754 portal ids are BOTH a `document_id` and a `version_id`** in
   `raw_versions`, so those two columns are not clean "act vs snapshot" roles.
   - **Unverified hypothesis worth testing first:** treat each `raw_versions` row as an
     edge `document_id <-> version_id`; the **connected components** of that graph are the
     Works, and each component's nodes are its expression pages. This would give a
     principled Work identity straight from the portal's own graph, instead of string
     matching. Confirm or refute it against the data.

## Things to explore (open-ended, not a checklist)

- How to identify a Work and group its expressions reliably. The version graph, the
  istoric, type+number+issuer+adoption-date: what actually holds up across the corpus?
- What a `raw_versions` row really represents, and how to derive a clean per-Work timeline
  with `valid_from` windows from the istoric dates (newest-first, current form unlinked).
- The full L2 schema: works, expressions, manifestations, articles, paragraphs,
  relationships. Which columns are facts vs derived? Where do articles/paragraphs hang
  (must be per-expression for point-in-time)? What are the keys and FKs?
- How point-in-time content reconstructs in an L3 view (valid_from <= X < next valid_from).
- The relationship target problem: ~27% of edges (74k/275k) point at ids not in the
  corpus. design.md theorizes most are "id-sprawl" (links to snapshot ids; base law lives
  under a different id). The version graph may resolve these via a snapshot->work map. Test it.
- Edge cases to keep honest: the 21k mistitled forms, the both-roles tangle, numberless/old
  acts keyed only by date, acts with no version history, the 403 unfetched snapshots.

## Where the current code lives

- `docs/design.md`: the user's own (unfinished) design notes. Read first. The
  "document_versions + version windows" item under *Parked & next* is the seed of this work.
- `etl/transform.py`: cleanup, parse, load orchestration.
- `etl/transforms/`: `text, dates, issuers, dedup, parse, relationships, quality`.
- `etl/extracts/`: `documents.py, relationships.py, versions.py` (how raw_* are built).
- `etl/load.py`, `etl/schemas.py`: current parquet writers plus Pandera contracts.

## Out of scope here (parked, separate build concerns)

The pipeline mechanics (sharding the parse, splitting the >2 GB FTS index into multiple
files, renaming workflows to `extract-documents` / `extract-relationships` / `publish`)
are downstream of the model. Ignore them while designing, but note the model's size drives
the FTS problem (keeping all versions roughly doubles the article count).

## Deliverable

A proposed L2 data model (tables, keys, FKs, what's fact vs derived), grounded in evidence
from the actual data, with the Work-identity and timeline questions resolved or clearly
scoped. Bias toward exploring and explaining the data over rushing to a schema.
