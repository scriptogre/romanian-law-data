# L2 data model (Akoma Ntoso shaped)

Version-aware model for the corpus. Decisions that hold are in the tables;
unsettled choices are in **Open questions**.

## Layers

| Layer | Holds | Form |
|---|---|---|
| L1 raw | verbatim source responses (DetaliiDocument HTML, relationship JSON, SOAP record) + a full-text index | parquet, FTS index |
| L2 core | normalized facts and structure, Akoma Ntoso shaped | parquet tables (below) |
| L3 views | convenience + temporal state, derived | SQL, materialized |

Altitude: **AKN shaped, not AKN-XML bound.** Borrow AKN's entity model,
vocabulary, identifiers, and point-in-time logic. Do not reproduce AKN XML
serialization for its own sake unless we later export AKN XML. Full text lives
in L1; L2 holds structure and facts for querying; L3 serves fast views.

## Conceptual shape (FRBR)

```
work            one legal act                         "Codul Civil"
└─ expression   one dated version of the act          Codul Civil as of 2013-04-29
   └─ element   one node of that version's body tree  art. 188, alin. (1), lit. a)
manifestation   one scraped portal page (a FRBR Item; links L2 to L1)
```

Work identity is earned from the data: the istoric graph (each `raw_versions`
row is an edge `document_id <-> version_id`) splits into connected components,
and each component is one **work**. See Evidence.

## Identity

Everything is addressed the AKN way. No surrogate integer ids.

- A whole document (work, expression) is keyed by its **`frbr_uri`**.
- A part of a document (element, modification, citation) is keyed by its
  document's **`frbr_uri` + an `eid`**. This is AKN's own `uri#eid` reference.
- A scraped page is a FRBR **Item** (AKN leaves Items out of scope), so it keeps
  the portal's own **`portal_id`**, a real source id, not a surrogate.

`frbr_uri` is derived from a work's identity facts (country, subtype, date,
number, or title where there is no number).

## Tables

### works
One row per legal act (FRBR Work).

```
PK frbr_uri        /akn/ro/act/lege/2009-07-17/287
   subtype         lege, ordonanta, hotarare, decret, codul, ...   [FRBRsubtype]
   number          287                                             [FRBRnumber]
   name            "Codul Civil"                                   [FRBRname]
   author          the issuer (Emitent)                            [FRBRauthor]
   date            the work (original) date                        [FRBRdate]
```

Country is constant (`ro`) and lives in the uri, so it is not a column.
`effective_at` (true in-force date, from the SOAP record) is a later enrichment.

### expressions
One row per consolidation date of a work (FRBR Expression).

```
PK frbr_uri        .../287/ron@2013-04-29
   work_uri        -> works.frbr_uri
   date            the consolidation date (valid_from)             [FRBRdate]
   language        ron                                             [FRBRlanguage]
```

No `valid_to`: L3 derives it from the next expression's date.

### elements
The full body hierarchy, one set per expression: book, title, chapter, section,
article, alinea, point. Mirrors the source's own tags (`S_CAP, S_SEC, S_ART,
S_ALN, S_LIT, S_PCT`); depth ends where the source stops tagging.

```
PK (expression_uri, eid)
   expression_uri  -> expressions.frbr_uri
   parent_eid      the containing element's eid; null at the body root
   type            book, title, chapter, section, article, alinea, point
   num             the element's number or letter
   heading         container heading (null for leaves)
   text            this node's OWN text only (null for pure containers)
   position        order within the expression
```

`parent_eid` is kept, not derived: AKN article eids are short (`art_188`, since
articles are act-unique), so an article's eid does not encode its chapter. The
parent link is the only place that structure lives.

### modifications
Amendment edges (AKN active/passive modifications). A modification is a part of
the source act's analysis, so it is addressed like any part: source uri + eid.
Endpoints reference the logical element `(work, eid)`; the root eid means the
whole act.

```
PK (source_work_uri, eid)        eid = mod_1, mod_2, ...
   source_work_uri -> works.frbr_uri
   source_eid                    the acting element; root = whole act
   target_work_uri -> works.frbr_uri    (null if target out of corpus)
   target_eid                    the affected element; root = whole act
   category                      textual, efficacy, ...
   type                          repeal, substitution, insertion, suspension, ...
   date                          when it takes effect (from the source act)
```

### citations
What an act cites (AKN references). Same shape as modifications.

```
PK (source_work_uri, eid)        eid = ref_1, ref_2, ...
   source_work_uri -> works.frbr_uri | source_eid
   target_work_uri -> works.frbr_uri | target_eid     (target_work_uri null if out of corpus)
```

(Named `citations`, not `references`: `REFERENCES` is a SQL reserved word.)

### manifestations
One row per scraped portal page (FRBR Item). The L2 to L1 link.

```
PK portal_id                     the DetaliiDocument id
   expression_uri  -> expressions.frbr_uri
   source_url
   format                        html                              [FRBRformat]
   retrieved_at
```

Duplicate pages of one expression are multiple manifestation rows. Full text and
HTML stay in L1, joined by `portal_id`.

## Example: one article

`works`:
```
frbr_uri                          subtype number name        author       date
/akn/ro/act/lege/2009-07-17/287   lege    287    Codul Civil Parlamentul  2009-07-17
```

`expressions`:
```
frbr_uri                                        work_uri                        date       language
/akn/ro/act/lege/2009-07-17/287/ron@2013-04-29  /akn/ro/act/lege/2009-07-17/287 2013-04-29 ron
```

`elements` (expression_uri = the expression above; text illustrative):
```
eid                   parent_eid           type     num  text / heading
book_1                (null)               book     I    "Despre persoane"
book_1__tit_2         book_1               title    II   "..."
book_1__tit_2__chp_1  book_1__tit_2        chapter  I    "..."
art_188               book_1__tit_2__chp_1 article  188  (none)
art_188__al_1         art_188              alinea   1    "Pot fi tutori:"
art_188__al_1__let_a  art_188__al_1        point    a    "soțul;"
art_188__al_1__let_b  art_188__al_1        point    b    "rudele minorului."
art_188__al_2         art_188              alinea   2    "Nu pot fi tutori: ..."
```

Each node holds only its own text; walk by `position` to rebuild the full text,
no duplication. The article's `parent_eid` shows it sits under chapter
`book_1__tit_2__chp_1`, which the short `art_188` eid alone cannot tell you.

## L3 views

- `point_in_time(date)`: the work's expression with the latest `date <= X`, then
  its elements. Dates before a work's first expression return nothing (honest
  source coverage).
- per-article history: `elements WHERE work=W AND eid=art_188` across expressions
  by date, plus `modifications WHERE target=(W, art_188)`.
- status: from incoming `modifications` (target = the work), ordered by date.

## Evidence (2026-06 corpus)

- istoric graph: 27,912 components (works); only 6 mix two act numbers (flag, do
  not auto-split). Title and type+number keys both fail; the graph holds.
- duplicates: 8,373 expressions have 2+ byte-identical pages (300/300 sampled
  pairs identical).
- relationship targets resolve 99.2% via the page-to-work map (was ~73%); 217
  distinct targets stay dangling.
- istoric lists amended forms only: for 14,511/14,517 dated one-version works the
  istoric date is after publication. Original pre-amendment text often has no
  page; the base-form endpoint (`?isFormaDeBaza=True`) can supply it.
- totals: ~257,801 works, ~293,169 expressions, ~302,480 manifestations.

## Open questions

1. **type vocabulary.** Final Romanian to AKN mapping for element types and
   modification types (the lists above are the working set).
2. **suspension / restoration.** Confirm the AKN category (forceMod vs
   efficacyMod) at implementation.
3. **modification date.** Confirm the source: the acting act's effective date
   (which should match the target expression's `date`).
4. **dangling targets (217).** `target_work_uri` null, or create stub works for
   referenced-but-uningested acts.
5. **cross-version element identity.** `(work, eid)` is stable while numbering is
   stable; renumbering and moved articles are the edge cases for per-article
   history.
6. **frbr_uri formation.** Numberless and old acts key on title + date rather
   than number; confirm the rule and uniqueness.

## Source and access

See `sources.md` for the source hierarchy (Monitorul Oficial to portal to
dataset), the portal endpoints, the SOAP record, legal basis, and the roadmap.
