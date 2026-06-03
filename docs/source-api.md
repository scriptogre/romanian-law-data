# legislatie.just.ro

How to get Romanian law. Two doors into the same site.

- **SOAP** returns the full text of every act, in bulk.
- **Web POST endpoints** return status, links between acts, and version dates. One act at a time.

Everything below is reverse-engineered. The site documents none of it.

---

## Door 1: SOAP (bulk)

`https://legislatie.just.ro/apiws/FreeWebService.svc?wsdl`

Two operations. That is all.

```
GetToken()                          -> token string
Search(SearchModel, tokenKey)       -> list of acts
```

Each act has **8 fields**:

```
Titlu  Text  TipAct  Numar  Emitent  Publicatie  DataVigoare  LinkHtml
```

`LinkHtml` ends in a number. That number is the act id. Use it everywhere below.

```
http://legislatie.just.ro/Public/DetaliiDocument/38070
                                                  ^^^^^ id
```

### Gotcha 1: the 403

The SOAP library sets `User-Agent: Zeep/...`. The site blocks it. Set a browser User-Agent **after** building the transport. See `etl/extract.py`.

### Gotcha 2: dead law looks alive

`Search` returns the whole archive. Repealed acts sit next to live ones. Nothing in the 8 fields says which is which.

```
CODUL PENAL  vig 1969-01-01   <- repealed in 2014, still returned
CODUL PENAL  vig 2014-02-01   <- the live one
```

To tell them apart, use Door 2.

---

## Door 2: web POST endpoints (status, links, versions)

Five endpoints. Each takes the act id. Each returns HTML inside JSON.

```
POST /Public/{endpoint}
body:  contor=38070
reply: { "acte": "<html...>" }
```

| Endpoint | Answers |
| --- | --- |
| `actiuniSuferite` | What happened to this act? (repealed, suspended, amended) |
| `actiuniInduse` | What does this act do to others? |
| `referitDe` | Who cites this act? |
| `referaPe` | What does this act cite? |
| `getReferitDe` | Same as `referitDe`, paged. |

### Read status from `actiuniSuferite`

Look at the rows that start with `Actul`.

```
Dead:  Actul ABROGAT DE    LEGE 187 24/10/2012
Live:  Actul MODIFICAT DE  DECIZIE 297 26/04/2018   (no ABROGAT row)
```

Rule:

```
ABROGAT DE present, no later REPUS  ->  abrogat
SUSPENDAT DE active                 ->  suspendat
otherwise                           ->  în vigoare
```

`MODIFICAT` and `COMPLETAT` do not change status.

The date is the date of the acting act. It is **not** always the date the act stopped applying. Treat it as a hint, not the truth.

### Read the link graph from `actiuniInduse`

Each row names a target act and links to its id. Build edges from here. Read from `actiuniInduse`, not `actiuniSuferite`: same edges, but each one appears once.

A Constitutional Court ruling shows up as a `MODIFICĂ` edge from a `DECIZIE`. That is the unconstitutionality link. No special handling.

---

## Versions

The act page holds a block `istoric_fa`: one dated link per consolidation, newest first.

```
<a href="/Public/DetaliiDocument/304554">19.12.2025</a>   past form, GET it
<a style="cursor:default">26.04.2026</a>                  current form, no link
```

Each link's id is a full act page. Fetch a past version like any other act:
`GET /Public/DetaliiDocument/{id}`. A version is valid from its date to the next-newer one.
