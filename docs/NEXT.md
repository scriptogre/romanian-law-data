# What's next

Pending work on the legislatie.just.ro enrichment. See `source-api.md` for the
endpoints these build on.

## Done

- **`status`** on `acte` (`în vigoare` / `abrogat` / `suspendat`). Derived from
  the `actiuniSuferite` HTML in `extract.py`, parsed in `transforms/status.py`,
  surfaced in the `acte` / `articole` / `alineate` views. Lets a query drop dead
  law: `WHERE status = 'în vigoare'`.

## Next

### 1. `relatii` table (the link graph)

The raw HTML is already fetched and stored in `raw_acts.jsonl` under
`actiuni_induse` (what each act does to others). No re-fetch needed. Parse it
into one row per edge:

```
relatii:  source_act_id, relationship_type, target_act_id, op_date
```

`relationship_type` comes from the row operation (ABROGĂ, MODIFICĂ, COMPLETEAZĂ,
SUSPENDĂ). A Constitutional Court ruling is a MODIFICĂ edge from a DECIZIE.

**Decide first (this is the blocker):** the edges point at other acts by their
legislatie doc id (the number in the link). Our tables use a surrogate `id`. So
either:

- store edges keyed by doc id and resolve to surrogate id in the view, or
- add a second pass that maps doc id to surrogate id after `acte` is written.

Pick one before writing code. This choice shapes the schema.

### 2. `valid_until` and version windows (phase 2)

`actiuniSuferite` gives a repeal date, but it is the date of the ACTING act, not
the date this act stopped applying. Do not store it as `valid_until`. The real
windows come from the `istoric_fa` block in the act page HTML: consecutive
consolidation dates are the boundaries. Needs new fetching (see `source-api.md`,
"Versions"). `valid_from` already exists as `effective_at`.

### 3. Per-article status and hierarchy (phase 2)

- Per-article repeal/amendment: the per-article rows of `actiuniSuferite`
  (`ART. 54 MODIFICAT DE ...`). Needs matching those rows to parsed articles.
- Structural hierarchy (Cartea / Titlul / Capitolul / Secțiunea): parse the
  headings from the `DetaliiDocumentAfis` page HTML.

### 4. Full corpus run

Run the new extract over the whole corpus. It is a long job (one extra POST per
act). Status data has a known lag of days to weeks behind the gazette.
