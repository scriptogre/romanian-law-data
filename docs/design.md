# Romanian legal corpus design notes

Model, source, open work.

Core tables: `documents`, `articles`, `paragraphs`, `relationships`.
Open: `document_versions`, `annexes`, article hierarchy (see *Parked & next*).

## Data model

Three layers:

| Layer | Holds | Lives in |
|---|---|---|
| raw | source response, verbatim | `raw_*.parquet` |
| core | clean, typed, lossless facts | core parquets below |
| views | joined, denormalized, convenient | `create_views.sql` |

Rule: recoverable facts → core. Lossy ones stay raw, smoothed in a view.

Surrogate integer PKs. Columns named `<level>_<role>` so joins stay unambiguous (`document_number` ≠ `article_number`).

### documents
One row per act: stable identity plus latest text.

```
id                surrogate PK
type              LEGE, OUG, HG, DECIZIE, CODUL PENAL, ...
document_number   nullable
document_citation "Legea 287/2009", "OUG 100/2024", "Codul Penal"
issuer
title
content           full body of latest version
adopted_at, published_at, effective_at
gazette_number    Monitorul Oficial issue number
status            in_force / repealed / suspended (see note)
link              portal URL (ends in portal id)
synced_at
```

`status` is materialised but belongs in a view: derived from `relationships`, temporal. See *Parked & next*.

### articles
One row per article, parsed from `documents.content`. Atom of legal content.

```
id                surrogate PK
document_id       -> documents
article_number    nullable
article_variant   bis / ter / ^1 / ...
article_citation  "Art. 188", "Art. 188 bis", "Articol unic"
content
```

### paragraphs
One row per alineat, parsed from `articles.content`. Only where the article subdivides; litere/puncte stay inline.

```
id                 surrogate PK
article_id         -> articles
paragraph_number   nullable
paragraph_citation "art. 188 alin. (1)"
content
```

### relationships
One row per directed edge: what one act does to another (source acts on target). Standalone table; targets can fall outside the corpus.

```
source_document_id  portal id of acting act (always in corpus)
target_document_id  portal id of target (may be uncorpused, see Known issue)
kind                repeals, amends, supplements, suspends, restores,
                    declares_unconstitutional, ... (English verb)
partial             bool, only part of target affected
scope               "act", or sub-unit (e.g. "ART. 4")
target_citation     raw "TYPE NUMBER DATE" string from source row
```

A Constitutional Court ruling is an `amends` / `declares_unconstitutional` edge from a DECIZIE. No date column; an edge's "when" is the source's `published_at`.

### views
`create_views.sql` exposes `documents` / `articles` / `paragraphs` plus pre-filtered lenses per canonical code (`constitution`, `civil_code`, `penal_code`, `labor_code`, `civil_procedure_code`, `penal_procedure_code`, `tax_code`) and `case_law`.

## Source: legislatie.just.ro

See `sources.md` for the source hierarchy, access (endpoints, SOAP, base form),
legal basis, and the Monitorul Oficial roadmap.

## Known issue: relationship targets that don't resolve

~27% of edges (74k/275k) carry a `target_document_id` we never scraped. Three causes, most recoverable:

1. **id-sprawl (the bulk, recoverable).** Link points at an *actualizat* / *republicată* snapshot id; the base law lives in the corpus under a different base id.
2. **genuine extraction gap.** Some base acts are absent (source completeness problem).
3. **numberless / old acts.** Keyed only by date, no number to match on (`REGULAMENT 26/06/2007`, `DECRET 377/1979`).

`short_title` is inconsistent within a type (`HG ...` vs `Hotărârea ...`) so it can't identify on its own.

**Fix:** resolve each target id via the snapshot→base id map from each act's `istoric_fa`. Genuinely-missing targets stay as raw citation strings (lossless). Natural-key matching (type+number+date) is unreliable: distinct acts can share a key.

## Parked & next

Downstream work. Revisit only after raw retrieval/extraction is solid.

- **Status as a view.** Drop the stored `status`; derive from `relationships` (incoming = `WHERE target_document_id = X`), ordering repeal vs restore by source `published_at`. No extra fetch. Blocked by target resolution.
- **`document_versions` + version windows.** Keep every dated consolidation. Timeline lives in `raw_versions`; fetching each snapshot id (`GET /Public/DetaliiDocument/{id}`) adds its text. Each version owns its articles/paragraphs, with `valid_from`/`valid_to` from consecutive istoric dates. The acting-act date from relationships is a hint only. Cost: large (~70 versions for the Penal Code). Difficulty: low.
- **Per-article status & hierarchy.** Per-article repeal/amendment from per-article `actiuniSuferite` rows (`ART. 54 MODIFICAT DE ...`). Structural hierarchy (Cartea / Titlul / Capitolul / Secțiunea) parsed from page headings into an article `ancestors` path. Substantive annexes (`ANEXA n`) into an `annexes` table.
