# Data sources

Where Romanian legislation comes from, what is legally reusable, and how to
obtain it. The project consumes legislatie.just.ro today; Monitorul Oficial is
the upstream of record, used later for authority and preservation.

## Source hierarchy

```
Monitorul Oficial, Partea I        legal source of truth (as-enacted)
   │   consolidated by Consiliul Legislativ (a constitutional body)
   ▼
legislatie.just.ro                 free, public-domain, machine-readable, versioned
   │   cleaned + structured + versioned by this project
   ▼
this open dataset                  queryable point-in-time legislation
```

- **Monitorul Oficial (MO), Partea I**: the only text with legal force. Each act
  is published once, as-enacted. MO does not consolidate.
- **legislatie.just.ro**: the consolidated, updated form, built by the Consiliul
  Legislativ. No legal force (a documentation instrument), but it carries the
  amendment timeline (istoric) that makes version history possible. It is the
  Partea I legislation subset of MO, plus consolidation.
- **lege5 / Indaco**: independent commercial consolidator from the same MO
  source. Copyrighted editorial work. Not a source for this project.

## Legal basis for republishing

- **Law 202/1998, art. 19(2)** (portal id 146886): the electronic format of MO
  Partea I and II is *gratuit și liber, permanent*, including for *căutare,
  salvare, distribuire și imprimare*. Redistribution is granted by statute.
- **legislatie.just.ro terms** (`/Public/Termeni`): content is public domain,
  reusable without restriction.
- Reuse of the content is lawful. Access is a separate matter: a host may still
  rate-limit or block automated collection regardless of reuse rights.

## legislatie.just.ro: access

The working door. Three endpoints, all keyed on the portal id (trailing number
of `LinkHtml`).

| Door | Call | Gives |
|---|---|---|
| Detail page | `GET /Public/DetaliiDocument/{id}` | act HTML: title, body, type, number, issuer, publication, istoric |
| Relationships | `POST /Public/{actiuniInduse,referaPe}` body `contor={id}` | outgoing edges (amends/repeals/cites) as HTML in JSON |
| SOAP | `Search(SearchNumar, SearchAn, SearchTitlu, SearchText)` | clean metadata incl. the true `DataVigoare` (in-force date) |

Enumeration is by sweeping the id space (~1..max); SOAP `Search` cannot
enumerate reliably (no stable sort, drops ~⅓), so it is used only as a targeted
per-act lookup. Full SOAP surface is `GetToken` + `Search` (the WSDL is the
complete contract; confirmed against the official docs).

Gotchas:
- SOAP rejects a Zeep `User-Agent` with 403; set a browser UA.
- The corpus mixes repealed and in-force acts; the page fields do not flag
  status. Status comes from the relationship edges, computed in L3.

Relationship endpoints come in inverse pairs. Fetch only the outgoing side; the
incoming side is its exact inverse (verified symmetric), derived in L2.

| Endpoint | Answers | Source |
|---|---|---|
| `actiuniInduse` | what this act does to others | fetched |
| `referaPe` | what this act cites | fetched |
| `actiuniSuferite` | what is done to this act | derived (inverse) |
| `referitDe` / `getReferitDe` | who cites this act | derived (inverse) |

Each edge row is `[scope, operation, target]`: the link carries the target's
portal id, the operation maps to a kind (amends, repeals, ...).

### Base (unconsolidated) form

`GET /Public/DetaliiDocument/{id}?isFormaDeBaza=True&rep=True` returns the act's
base form: the text before consolidated amendments. It is smaller than the
default consolidated page (Penal Code 446 vs 458 articles, 1.67 vs 1.97 MB;
Ordin 849/2003 77 vs 94 KB) and still carries the title and istoric. The
frontend reaches it via `POST /Public/DetaliiDocument/{id}` body
`isFormaDeBaza=true`, which replies `{"Url": "...?isFormaDeBaza=True&rep=True"}`.

A same-page query param, not a separate door. It yields original text for acts
whose pre-amendment form has no page of its own. Which historical point the base
form represents per act is not yet confirmed.

`DetaliiDocumentAfis/{id}` and `FormaPrintabila` are other render endpoints the
frontend references; not characterized.

## Monitorul Oficial: access

The upstream of record. Harder to consume, needed only for authority and
preservation (see roadmap Phase 3).

- **e-monitor** (`monitoruloficial.ro/e-monitor`): free PDF, same-day, no
  watermark. Partea I and II, 2000 to present. Expanding toward 1990. Pre-2000
  only by paid scan order (`comenzimo@ramo.ro`, ~2 lei/page).
- Output is whole-issue PDFs (many acts per file), not structured data. No
  consolidation, no per-act addressing.
- **Stack**: WordPress + WooCommerce + Wordfence, on Rocky Linux 8 + old Apache.
  The site sells subscriptions via WooCommerce.
- **No usable API for the archive.** `/wp-json/` is live but exposes only the
  CMS and shop (`post`, `page`, `product`); there is no legal-act post type. The
  gazette PDFs sit behind a search UI guarded by Wordfence, not in the API.

### Obtaining the MO corpus (Phase 3)

Prefer mirrors over scraping; the content is identical and WAF-free.

- **Wikimedia Commons** already hosts Partea I issue PDFs (public domain).
- **Academic Torrents**: a community archive of the full PDF set is reported in
  progress; collaboration avoids re-harvesting.
- A community holder reports Partea I complete to ~2024.

If direct collection is necessary, Wordfence triggers on per-IP request volume.
Reported working approaches: a polite low request rate (seconds between hits);
rotating residential proxies or a rotating commercial VPN; or short-lived cloud
VMs cycled for fresh IPs (note ~1h minimum billing). A plain `curl` or a single
headless browser from one IP gets blocked.

After collection the work is still large: OCR, segment each issue into individual
acts, classify, and match to the consolidated acts. Do this only for specific
gaps (missing originals, pre-2000), never as a full re-consolidation.

## Roadmap

```
Phase 1 (now)   legislatie.just.ro -> clean L1/L2/L3: structure, versions,
                point-in-time, SOAP in-force dates, resolved relationships.

Phase 2         Link every act to its MO e-monitor PDF for authoritative
                provenance. Cheap: each act already carries its MO reference
                (Publicatie). Optionally archive the PDFs for preservation.

Phase 3+        Ingest MO PDFs only for specific gaps (missing originals,
                pre-2000), via mirrors. Scoped backfill, not re-consolidation.
```

Archiving MO PDFs (cheap, preservation) is distinct from structuring them into
data (expensive, mostly redundant with the portal's consolidation). If MO is
ever pursued, favour the first.
