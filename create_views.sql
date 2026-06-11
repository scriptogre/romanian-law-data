-- ============================================================================
-- LLM-facing surface for the Romanian legal corpus.
--
-- Mental model: three concentric levels of legal text, each with a citation
-- string at that level. Naming follows `<level>_<role>` so every column says
-- which level it describes:
--
--   document   — a whole law, code, OUG, hotărâre, decizie, etc.
--   article    — one article inside a document  (parent: document)
--   paragraph  — one paragraph inside an article (parent: article, ancestor: document)
--
-- Each level has its own table and its own citation column:
--   documents.document_citation             "Codul Penal", "Legea 287/2009", "OUG 100/2024"
--   articles.article_citation     "Art. 188", "Art. 188 bis", "Art. 188^1"
--   paragraphs.paragraph_citation   "Art. 188 alin. (1)" (includes parent article)
--
-- `articles` and `paragraphs` are pre-JOIN-ed: each row already carries its
-- parent document's citation + link, so a single SELECT returns everything needed
-- to compose a chip citation in the answer. No manual JOIN required.
--
-- Subject views (constitution, civil_code, penal_code, ...) are filters on
-- `documents` that select the single forma-în-vigoare row of each code. Use them
-- to scope an article/paragraph query to a specific code:
--   WHERE document_id IN (SELECT id FROM penal_code)
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- DOCUMENTS — un rând per act normativ
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW documents AS
SELECT
    id,
    type,
    document_number,
    document_citation,
    issuer,
    title,
    content,
    adopted_at,
    published_at,
    effective_at,
    gazette_number,
    status,
    link
FROM read_parquet('data/documents.parquet');

COMMENT ON VIEW documents IS
'Acte normative din corpusul juridic român. Sursa: legislatie.just.ro (Ministerul Justiției). Un rând per act distinct. Acoperă LEGI, ORDONANȚE (OUG, OG), HOTĂRÂRI DE GUVERN (HG), ORDINE ministeriale, DECRETE prezidențiale, DECIZII și HOTĂRÂRI ale Curții Constituționale (CCR) și ÎCCJ, plus documente conexe (RAPORT, COMUNICAT, RECTIFICARE, CUANTUM TOTAL) și codurile (CODUL CIVIL, CODUL PENAL, CONSTITUȚIE, etc.). Pentru regăsire la nivel de articol sau alineat, NU JOIN-ui manual cu articles / paragraphs — interoghează direct view-urile articles / paragraphs, care includ deja contextul actului (document_citation, link).';

COMMENT ON COLUMN documents.id IS
'Cheie primară surogat, generată de pipeline. Referită de articles.document_id (intern, deja JOIN-uit în view).';

COMMENT ON COLUMN documents.type IS
'Tipul actului așa cum este clasificat în Monitorul Oficial. Valori frecvente: LEGE, ORDONANȚĂ DE URGENȚĂ, ORDONANȚĂ, HOTĂRÂRE, ORDIN, DECRET, DECIZIE, ÎNCHEIERE, SENTINȚĂ, COMUNICAT, RAPORT, RECTIFICARE, CUANTUM TOTAL, NORMĂ, METODOLOGIE, REGULAMENT, INSTRUCȚIUNI, CIRCULARĂ, ANEXĂ, CONSTITUȚIE, CODUL CIVIL, CODUL PENAL etc. Acesta este principalul câmp pentru filtrarea după natura documentului.';

COMMENT ON COLUMN documents.document_number IS
'Numărul actului în formă brută, exact cum vine din SOAP-ul legislatie.just.ro: de obicei doar numărul ("287", "75"), uneori "număr/an" ("286/2009"). NULL pentru documente fără număr distinct (CODURI și CONSTITUȚIE, COMUNICAT-uri ÎCCJ, RAPORT-uri, CUANTUM TOTAL, RECTIFICARI). NU folosi această coloană pentru lookup după citarea folosită de juriști — pentru asta folosește document_citation.';

COMMENT ON COLUMN documents.document_citation IS
'Citarea actului în forma pe care o folosesc juriștii români: "Legea 287/2009", "OUG 100/2024", "HG 405/2026", "Ordinul 744/2026", "Decretul 251/2026", "Decizia 175/2025", "Codul Civil", "Codul Penal", "Constituția României". Calculată din (type, document_number, adopted_at, issuer) la export. ACEASTA ESTE COLOANA DE FOLOSIT pentru lookup direct după o referință legală cunoscută — ex: `WHERE document_citation = ''Legea 287/2009''`. Pentru coduri și Constituție conține numele canonic; mai multe republicări istorice pot împărți aceeași citare — pentru forma în vigoare folosește view-urile dedicate (constitution, civil_code, penal_code, etc.).';

COMMENT ON COLUMN documents.issuer IS
'Autoritatea emitentă, în majuscule, exact cum apare în antetul actului din Monitorul Oficial. Exemple: "PARLAMENTUL ROMÂNIEI" (legi), "GUVERNUL ROMÂNIEI" (OUG, OG, HG), "MINISTERUL JUSTIȚIEI", "CURTEA CONSTITUȚIONALĂ" (decizii CCR), "ÎNALTA CURTE DE CASAȚIE ȘI JUSTIȚIE" (decizii ÎCCJ), "PREȘEDINTELE ROMÂNIEI" (decrete). Pentru ordine comune, conține toți emitenții separați prin " / " — ex: "MINISTERUL FINANȚELOR / MINISTERUL DEZVOLTĂRII".';

COMMENT ON COLUMN documents.title IS
'Titlul oficial al actului, curățat de antetul tehnic ("EMITENT ... PUBLICAT ÎN ...") care apare în răspunsul SOAP. Conține de obicei tipul, numărul, data adoptării și obiectul reglementării. Exemplu: "LEGE nr. 75 din 21 mai 2026 pentru modificarea art. 597 din Legea nr. 135/2010 privind Codul de procedură penală".';

COMMENT ON COLUMN documents.content IS
'Textul integral al actului, în format text simplu. Câmp mare — poate depăși 100 KB pentru acte voluminoase (coduri, anexe extinse). Pentru regăsire structurată pe articol sau alineat, NU folosi acest câmp; folosește view-urile articles și paragraphs, care conțin aceleași informații parsate ierarhic și gata JOIN-uite cu actul-părinte.';

COMMENT ON COLUMN documents.adopted_at IS
'Data adoptării / semnării actului, extrasă din titlu (ex: "LEGE nr. 75 din 21 mai 2026" → 2026-05-21). Pentru legi: votul final în Parlament. Pentru OUG / HG: ședința de Guvern. Pentru ordine ministeriale: semnătura ministrului. Pentru decizii CCR / ÎCCJ: pronunțarea. NULL când tiparul nu a putut fi extras.';

COMMENT ON COLUMN documents.published_at IS
'Data publicării în Monitorul Oficial al României, Partea I. Aceasta este data folosită canonic pentru citarea formală ("Legea nr. 75/2026 publicată în M.Of. nr. 431 din 21 mai 2026"). Extrasă din antetul textului. De obicei coincide cu adopted_at sau este la 1-7 zile mai târziu. NULL pentru documente nepublicate în M.Of. — în special COMUNICATE-le ÎCCJ, care apar doar pe scj.ro.';

COMMENT ON COLUMN documents.effective_at IS
'Data intrării în vigoare. Conform art. 78 din Constituție, legile intră în vigoare la 3 zile de la publicarea în M.Of., dacă nu specifică altă dată. HG-urile cu caracter individual intră în vigoare imediat la publicare. Deciziile CCR sunt obligatorii de la publicarea în M.Of. Sursa: câmpul DataVigoare din SOAP. Răspunde la întrebarea "este actul în vigoare?".';

COMMENT ON COLUMN documents.gazette_number IS
'Numărul de Monitor Oficial al României, Partea I, în care a fost publicat actul (ex: "M.Of. nr. 431" → 431). Folosit împreună cu published_at pentru citarea formală completă. NULL când tiparul nu a putut fi extras din antet.';

COMMENT ON COLUMN documents.status IS
'Starea actului în ciclul său de viață: "în vigoare", "abrogat" (abrogat de un alt act, fără repunere ulterioară) sau "suspendat". Derivată din acțiunile suferite de act (endpoint-ul actiuniSuferite de pe legislatie.just.ro). ACEASTA ESTE COLOANA care răspunde la "mai este actul în vigoare?" — un act poate exista în corpus dar să fie abrogat. Pentru a exclude legislația moartă: `WHERE status = ''în vigoare''`. NULL = stare necunoscută (datele de stare nu au fost preluate pentru acest act). Atenție: reflectă consolidarea Ministerului Justiției, care poate avea întârzieri de zile/săptămâni față de Monitorul Oficial.';

COMMENT ON COLUMN documents.link IS
'URL absolut către pagina actului pe legislatie.just.ro. Format: http://legislatie.just.ro/Public/DetaliiDocument/{id}. Aceeași valoare apare și în articles.link / paragraphs.link pentru actul-părinte, ca să poți construi link-ul de citare direct dintr-o singură interogare la nivel de articol sau alineat.';


-- ────────────────────────────────────────────────────────────────────────────
-- ARTICLES — un rând per articol, JOIN-uit deja cu actul-părinte
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW articles AS
SELECT
    ar.id,
    ar.document_id,
    a.document_citation,
    a.status,
    a.link,
    ar.article_number,
    ar.article_variant,
    ar.article_citation,
    ar.content
FROM read_parquet('data/articles.parquet') ar
JOIN read_parquet('data/documents.parquet') a ON a.id = ar.document_id;

COMMENT ON VIEW articles IS
'Articolele extrase din fiecare act. Un rând per articol. Vine JOIN-uit deja cu actul-părinte: document_citation și link sunt incluse direct, nu trebuie să faci JOIN cu documents. Pentru actele care nu sunt structurate pe articole (decizii, comunicate, rapoarte), un singur rând cu article_number IS NULL și content egal cu textul întreg al actului. Pentru regăsire la nivel de alineat folosește view-ul paragraphs.';

COMMENT ON COLUMN articles.id IS
'Cheie primară surogat. Referită de paragraphs.article_id (intern, deja JOIN-uit în view-ul paragraphs).';

COMMENT ON COLUMN articles.document_id IS
'FK către documents.id. Folosește-o pentru a restrânge la articolele unui anume act, ex: `WHERE document_id IN (SELECT id FROM penal_code)`.';

COMMENT ON COLUMN articles.document_citation IS
'Citarea actului-părinte (preluată din documents.document_citation). Pereche cu `link` formează chip-ul de citare în răspuns. Exemple: "Codul Penal", "Legea 287/2009". Vezi documents.document_citation pentru semantică completă.';

COMMENT ON COLUMN articles.status IS
'Starea actului-părinte (preluată din documents.status): "în vigoare", "abrogat" sau "suspendat". Filtrează articolele din legislația moartă: `WHERE status = ''în vigoare''`. Vezi documents.status pentru semantică completă. NULL = stare necunoscută.';

COMMENT ON COLUMN articles.link IS
'URL-ul actului-părinte pe legislatie.just.ro (preluat din documents.link). Folosit împreună cu document_citation pentru a construi link markdown în răspuns.';

COMMENT ON COLUMN articles.article_number IS
'Numărul articolului ca întreg ordinal (188 pentru "art. 188", "Art. 188", "Articolul 188"). Pentru articolele cu variantă (188 bis, 188^1), numărul de bază stă aici și sufixul în article_variant. NULL doar pentru actele nestructurate pe articles.';

COMMENT ON COLUMN articles.article_variant IS
'Sufixul de variantă al articolului, când există. Valori observate: "bis", "ter", "quater", "quinquies", "sexies", "septies", "octies" (notație latină), sau "^1", "^2", "^3" ... (notație indice). NULL pentru articolele standard. Permite distincția între "Art. 188", "Art. 188 bis" și "Art. 188^1" — articole DIFERITE introduse ulterior între numere consecutive fără renumerotare.';

COMMENT ON COLUMN articles.article_citation IS
'Citarea articolului în forma în care o scrie un jurist român, gata de afișat: "Art. 188", "Art. 188 bis", "Art. 188^1". Conține DOAR referința articolului — actul-părinte stă în document_citation. Pentru actele nestructurate conține valoarea literal "(unparsed)".';

COMMENT ON COLUMN articles.content IS
'Textul integral al articolului — toate alineatele concatenate, în ordine. Pentru regăsire mai fină pe alineat, folosește view-ul paragraphs.';


-- ────────────────────────────────────────────────────────────────────────────
-- PARAGRAPHS — un rând per alineat, JOIN-uit deja cu actul-părinte
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW paragraphs AS
SELECT
    al.id,
    al.article_id,
    ar.document_id,
    a.document_citation,
    a.status,
    a.link,
    ar.article_number,
    ar.article_variant,
    ar.article_citation,
    al.paragraph_number,
    al.paragraph_citation,
    al.content
FROM read_parquet('data/paragraphs.parquet') al
JOIN read_parquet('data/articles.parquet') ar ON ar.id = al.article_id
JOIN read_parquet('data/documents.parquet') a ON a.id = ar.document_id;

COMMENT ON VIEW paragraphs IS
'Alineatele extrase din fiecare articol. Un rând per alineat. ACEASTA ESTE UNITATEA CEA MAI FINĂ DE CITARE — corespunde cu "art. 188 alin. (1)" din practica juridică. Vine JOIN-uit deja cu actul-părinte și articolul-părinte: document_citation, link și article_citation sunt incluse direct. Pentru articolele monolitice (fără alineate distincte (1), (2), (3) ...), conține un singur rând cu paragraph_number IS NULL și content egal cu articolul întreg.';

COMMENT ON COLUMN paragraphs.id IS
'Cheie primară surogat.';

COMMENT ON COLUMN paragraphs.article_id IS
'FK către articles.id. Folosește-o când vrei să iei toate alineatele unui articol specific.';

COMMENT ON COLUMN paragraphs.status IS
'Starea actului-părinte (preluată din documents.status): "în vigoare", "abrogat" sau "suspendat". Filtrează alineatele din legislația moartă: `WHERE status = ''în vigoare''`. Vezi documents.status pentru semantică completă. NULL = stare necunoscută.';

COMMENT ON COLUMN paragraphs.document_id IS
'FK către documents.id (transitiv prin articole). Folosește-o pentru a restrânge la alineatele dintr-un anume act, ex: `WHERE document_id IN (SELECT id FROM penal_code)`.';

COMMENT ON COLUMN paragraphs.document_citation IS
'Citarea actului-părinte. Pereche cu `link` formează chip-ul de citare în răspuns.';

COMMENT ON COLUMN paragraphs.link IS
'URL-ul actului-părinte pe legislatie.just.ro.';

COMMENT ON COLUMN paragraphs.article_number IS
'Numărul articolului-părinte (preluat din articles.article_number). Folosește-l ca să filtrezi alineatele unui articol specific: `WHERE article_number = 188`.';

COMMENT ON COLUMN paragraphs.article_variant IS
'Sufixul de variantă al articolului-părinte (preluat din articles.article_variant), ex: "bis", "^1". NULL pentru articolele standard.';

COMMENT ON COLUMN paragraphs.article_citation IS
'Citarea articolului-părinte (ex: "Art. 188"). Coloană suplimentară față de paragraph_citation, în cazul în care vrei doar referința articolului, nu și a alineatului.';

COMMENT ON COLUMN paragraphs.paragraph_number IS
'Numărul alineatului (1, 2, 3 ...) — exact numărul din "(1)", "(2)", "(3)". NULL = articolul nu este împărțit în alineate; conținutul rândului este articolul în întregime.';

COMMENT ON COLUMN paragraphs.paragraph_citation IS
'Citarea completă a alineatului în forma juridică românească: "Art. 188 alin. (1)". Include și referința articolului. Pentru articolele fără alineate (paragraph_number IS NULL), egal cu citarea articolului (ex: "Art. 188").';

COMMENT ON COLUMN paragraphs.content IS
'Textul alineatului, fără markeri inițiali "(N)". Pentru articolele fără alineate, textul articolului întreg.';


-- ────────────────────────────────────────────────────────────────────────────
-- VIEW-URI PE COD — selectează forma în vigoare pentru fiecare cod și pentru Constituție
-- ────────────────────────────────────────────────────────────────────────────
-- Fiecare cod român major are un view dedicat care selectează singurul rând
-- (forma în vigoare) din `documents`. Folosește-le pentru a restrânge un query la
-- nivel de articol sau alineat la un anume cod:
--   WHERE document_id IN (SELECT id FROM penal_code)
--
-- legislatie.just.ro stochează fiecare cod consolidat sub un TipAct dedicat
-- (CODUL CIVIL, CODUL PENAL, etc.), nu sub LEGE. View-urile pe cod filtrează
-- pe TipAct + anul adoptării. Codurile cu mai multe republicări (cod proc.
-- civilă) sunt dezambiguate selectând rândul cu cel mai mult conținut.

CREATE OR REPLACE VIEW constitution AS
SELECT * FROM documents
 WHERE type = 'CONSTITUȚIE'
   AND EXTRACT(YEAR FROM adopted_at) = 1991
   AND title ILIKE '%republicat%';

COMMENT ON VIEW constitution IS
'Constituția României în vigoare. Forma republicată în 2003. Un singur rând. Pentru articole specifice, folosește `WHERE document_id IN (SELECT id FROM constitution)` în view-ul articles.';


CREATE OR REPLACE VIEW civil_code AS
SELECT * FROM documents
 WHERE type = 'CODUL CIVIL'
   AND EXTRACT(YEAR FROM adopted_at) = 2009
   AND title ILIKE '%republicat%';

COMMENT ON VIEW civil_code IS
'Codul Civil al României în vigoare. Sursa: Legea nr. 287/2009, republicată. Reglementează raporturile civile între persoane: contracte, obligații, drepturi reale, succesiuni, familie. Un singur rând.';


CREATE OR REPLACE VIEW penal_code AS
SELECT * FROM documents
 WHERE type = 'CODUL PENAL'
   AND EXTRACT(YEAR FROM adopted_at) = 2009;

COMMENT ON VIEW penal_code IS
'Codul Penal al României în vigoare. Sursa: Legea nr. 286/2009. Definește infracțiunile, pedepsele și răspunderea penală. Un singur rând.';


CREATE OR REPLACE VIEW labor_code AS
SELECT * FROM documents
 WHERE type = 'CODUL MUNCII'
   AND EXTRACT(YEAR FROM adopted_at) = 2003
   AND title ILIKE '%republicat%';

COMMENT ON VIEW labor_code IS
'Codul Muncii al României în vigoare. Sursa: Legea nr. 53/2003, republicată. Reglementează raporturile individuale și colective de muncă. Un singur rând.';


CREATE OR REPLACE VIEW civil_procedure_code AS
SELECT * FROM documents
 WHERE type = 'CODUL DE PROCEDURĂ CIVILĂ'
   AND EXTRACT(YEAR FROM adopted_at) = 2010
   AND title ILIKE '%republicat%'
 ORDER BY LENGTH(content) DESC
 LIMIT 1;

COMMENT ON VIEW civil_procedure_code IS
'Codul de Procedură Civilă al României în vigoare. Sursa: Legea nr. 134/2010, republicată. Reglementează procedura judecății în materie civilă. Un singur rând (versiunea cu cel mai mult conținut, dintre republicări).';


CREATE OR REPLACE VIEW penal_procedure_code AS
SELECT * FROM documents
 WHERE type = 'CODUL DE PROCEDURĂ PENALĂ'
   AND EXTRACT(YEAR FROM adopted_at) = 2010;

COMMENT ON VIEW penal_procedure_code IS
'Codul de Procedură Penală al României în vigoare. Sursa: Legea nr. 135/2010. Reglementează procedura judecății în materie penală. Un singur rând.';


CREATE OR REPLACE VIEW tax_code AS
SELECT * FROM documents
 WHERE type = 'CODUL FISCAL'
   AND EXTRACT(YEAR FROM adopted_at) = 2015;

COMMENT ON VIEW tax_code IS
'Codul Fiscal al României în vigoare. Sursa: Legea nr. 227/2015. Reglementează impozitele, taxele și contribuțiile sociale. Un singur rând.';


-- ────────────────────────────────────────────────────────────────────────────
-- JURISPRUDENȚĂ
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW case_law AS
SELECT *
  FROM documents
 WHERE type IN ('DECIZIE', 'HOTĂRÂRE', 'ÎNCHEIERE', 'SENTINȚĂ')
   AND (issuer LIKE 'CURTEA CONSTITUȚIONALĂ%'
        OR issuer LIKE 'ÎNALTA CURTE DE CASAȚIE ȘI JUSTIȚIE%');

COMMENT ON VIEW case_law IS
'Hotărâri și decizii ale instanțelor supreme din România: Curtea Constituțională (CCR) și Înalta Curte de Casație și Justiție (ÎCCJ, inclusiv secțiile sale). Filtru pe type IN (DECIZIE, HOTĂRÂRE, ÎNCHEIERE, SENTINȚĂ) ȘI issuer corespunzător. NU include actele administrative ale acestor instanțe (COMUNICAT, RAPORT). Pentru paragrafele unei decizii folosește view-ul paragraphs cu `WHERE document_id IN (SELECT id FROM case_law WHERE ...)`.';
