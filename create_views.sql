-- ============================================================================
-- LLM-facing surface for the Romanian legal corpus.
--
-- Mental model: three concentric levels of legal text, each with a citation
-- string at that level. Naming follows `<level>_<role>` so every column says
-- which level it describes:
--
--   act        — a whole document (law, code, OUG, hotărâre, decizie, etc.)
--   article    — one article inside an act     (parent: act)
--   paragraph  — one paragraph inside article  (parent: article, ancestor: act)
--
-- Each level has its own table and its own citation column:
--   acte.act_citation             "Codul Penal", "Legea 287/2009", "OUG 100/2024"
--   articole.article_citation     "Art. 188", "Art. 188 bis", "Art. 188^1"
--   alineate.paragraph_citation   "Art. 188 alin. (1)" (includes parent article)
--
-- `articole` and `alineate` are pre-JOIN-ed: each row already carries its
-- parent act's citation + link, so a single SELECT returns everything needed
-- to compose a chip citation in the answer. No manual JOIN required.
--
-- Subject views (constitutie, cod_civil, cod_penal, ...) are filters on
-- `acte` that select the single forma-în-vigoare row of each code. Use them
-- to scope an article/paragraph query to a specific code:
--   WHERE act_id IN (SELECT id FROM cod_penal)
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- ACTE — un rând per act normativ
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW acte AS
SELECT
    id,
    type,
    act_number,
    act_citation,
    issuer,
    title,
    content,
    adopted_at,
    published_at,
    effective_at,
    gazette_number,
    link
FROM read_parquet('data/laws/acte.parquet');

COMMENT ON VIEW acte IS
'Acte normative din corpusul juridic român. Sursa: legislatie.just.ro (Ministerul Justiției). Un rând per act distinct. Acoperă LEGI, ORDONANȚE (OUG, OG), HOTĂRÂRI DE GUVERN (HG), ORDINE ministeriale, DECRETE prezidențiale, DECIZII și HOTĂRÂRI ale Curții Constituționale (CCR) și ÎCCJ, plus documente conexe (RAPORT, COMUNICAT, RECTIFICARE, CUANTUM TOTAL) și codurile (CODUL CIVIL, CODUL PENAL, CONSTITUȚIE, etc.). Pentru regăsire la nivel de articol sau alineat, NU JOIN-ui manual cu articole / alineate — interoghează direct view-urile articole / alineate, care includ deja contextul actului (act_citation, link).';

COMMENT ON COLUMN acte.id IS
'Cheie primară surogat, generată de pipeline. Referită de articole.act_id (intern, deja JOIN-uit în view).';

COMMENT ON COLUMN acte.type IS
'Tipul actului așa cum este clasificat în Monitorul Oficial. Valori frecvente: LEGE, ORDONANȚĂ DE URGENȚĂ, ORDONANȚĂ, HOTĂRÂRE, ORDIN, DECRET, DECIZIE, ÎNCHEIERE, SENTINȚĂ, COMUNICAT, RAPORT, RECTIFICARE, CUANTUM TOTAL, NORMĂ, METODOLOGIE, REGULAMENT, INSTRUCȚIUNI, CIRCULARĂ, ANEXĂ, CONSTITUȚIE, CODUL CIVIL, CODUL PENAL etc. Acesta este principalul câmp pentru filtrarea după natura documentului.';

COMMENT ON COLUMN acte.act_number IS
'Numărul actului în formă brută, exact cum vine din SOAP-ul legislatie.just.ro: de obicei doar numărul ("287", "75"), uneori "număr/an" ("286/2009"). NULL pentru documente fără număr distinct (CODURI și CONSTITUȚIE, COMUNICAT-uri ÎCCJ, RAPORT-uri, CUANTUM TOTAL, RECTIFICARI). NU folosi această coloană pentru lookup după citarea folosită de juriști — pentru asta folosește act_citation.';

COMMENT ON COLUMN acte.act_citation IS
'Citarea actului în forma pe care o folosesc juriștii români: "Legea 287/2009", "OUG 100/2024", "HG 405/2026", "Ordinul 744/2026", "Decretul 251/2026", "Decizia 175/2025", "Codul Civil", "Codul Penal", "Constituția României". Calculată din (type, act_number, adopted_at, issuer) la export. ACEASTA ESTE COLOANA DE FOLOSIT pentru lookup direct după o referință legală cunoscută — ex: `WHERE act_citation = ''Legea 287/2009''`. Pentru coduri și Constituție conține numele canonic; mai multe republicări istorice pot împărți aceeași citare — pentru forma în vigoare folosește view-urile dedicate (constitutie, cod_civil, cod_penal, etc.).';

COMMENT ON COLUMN acte.issuer IS
'Autoritatea emitentă, în majuscule, exact cum apare în antetul actului din Monitorul Oficial. Exemple: "PARLAMENTUL ROMÂNIEI" (legi), "GUVERNUL ROMÂNIEI" (OUG, OG, HG), "MINISTERUL JUSTIȚIEI", "CURTEA CONSTITUȚIONALĂ" (decizii CCR), "ÎNALTA CURTE DE CASAȚIE ȘI JUSTIȚIE" (decizii ÎCCJ), "PREȘEDINTELE ROMÂNIEI" (decrete). Pentru ordine comune, conține toți emitenții separați prin " / " — ex: "MINISTERUL FINANȚELOR / MINISTERUL DEZVOLTĂRII".';

COMMENT ON COLUMN acte.title IS
'Titlul oficial al actului, curățat de antetul tehnic ("EMITENT ... PUBLICAT ÎN ...") care apare în răspunsul SOAP. Conține de obicei tipul, numărul, data adoptării și obiectul reglementării. Exemplu: "LEGE nr. 75 din 21 mai 2026 pentru modificarea art. 597 din Legea nr. 135/2010 privind Codul de procedură penală".';

COMMENT ON COLUMN acte.content IS
'Textul integral al actului, în format text simplu. Câmp mare — poate depăși 100 KB pentru acte voluminoase (coduri, anexe extinse). Pentru regăsire structurată pe articol sau alineat, NU folosi acest câmp; folosește view-urile articole și alineate, care conțin aceleași informații parsate ierarhic și gata JOIN-uite cu actul-părinte.';

COMMENT ON COLUMN acte.adopted_at IS
'Data adoptării / semnării actului, extrasă din titlu (ex: "LEGE nr. 75 din 21 mai 2026" → 2026-05-21). Pentru legi: votul final în Parlament. Pentru OUG / HG: ședința de Guvern. Pentru ordine ministeriale: semnătura ministrului. Pentru decizii CCR / ÎCCJ: pronunțarea. NULL când tiparul nu a putut fi extras.';

COMMENT ON COLUMN acte.published_at IS
'Data publicării în Monitorul Oficial al României, Partea I. Aceasta este data folosită canonic pentru citarea formală ("Legea nr. 75/2026 publicată în M.Of. nr. 431 din 21 mai 2026"). Extrasă din antetul textului. De obicei coincide cu adopted_at sau este la 1-7 zile mai târziu. NULL pentru documente nepublicate în M.Of. — în special COMUNICATE-le ÎCCJ, care apar doar pe scj.ro.';

COMMENT ON COLUMN acte.effective_at IS
'Data intrării în vigoare. Conform art. 78 din Constituție, legile intră în vigoare la 3 zile de la publicarea în M.Of., dacă nu specifică altă dată. HG-urile cu caracter individual intră în vigoare imediat la publicare. Deciziile CCR sunt obligatorii de la publicarea în M.Of. Sursa: câmpul DataVigoare din SOAP. Răspunde la întrebarea "este actul în vigoare?".';

COMMENT ON COLUMN acte.gazette_number IS
'Numărul de Monitor Oficial al României, Partea I, în care a fost publicat actul (ex: "M.Of. nr. 431" → 431). Folosit împreună cu published_at pentru citarea formală completă. NULL când tiparul nu a putut fi extras din antet.';

COMMENT ON COLUMN acte.link IS
'URL absolut către pagina actului pe legislatie.just.ro. Format: http://legislatie.just.ro/Public/DetaliiDocument/{id}. Aceeași valoare apare și în articole.link / alineate.link pentru actul-părinte, ca să poți construi link-ul de citare direct dintr-o singură interogare la nivel de articol sau alineat.';


-- ────────────────────────────────────────────────────────────────────────────
-- ARTICOLE — un rând per articol, JOIN-uit deja cu actul-părinte
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW articole AS
SELECT
    ar.id,
    ar.act_id,
    a.act_citation,
    a.link,
    ar.article_number,
    ar.article_variant,
    ar.article_citation,
    ar.content
FROM read_parquet('data/laws/articole.parquet') ar
JOIN read_parquet('data/laws/acte.parquet') a ON a.id = ar.act_id;

COMMENT ON VIEW articole IS
'Articolele extrase din fiecare act. Un rând per articol. Vine JOIN-uit deja cu actul-părinte: act_citation și link sunt incluse direct, nu trebuie să faci JOIN cu acte. Pentru actele care nu sunt structurate pe articole (decizii, comunicate, rapoarte), un singur rând cu article_number IS NULL și content egal cu textul întreg al actului. Pentru regăsire la nivel de alineat folosește view-ul alineate.';

COMMENT ON COLUMN articole.id IS
'Cheie primară surogat. Referită de alineate.article_id (intern, deja JOIN-uit în view-ul alineate).';

COMMENT ON COLUMN articole.act_id IS
'FK către acte.id. Folosește-o pentru a restrânge la articolele unui anume act, ex: `WHERE act_id IN (SELECT id FROM cod_penal)`.';

COMMENT ON COLUMN articole.act_citation IS
'Citarea actului-părinte (preluată din acte.act_citation). Pereche cu `link` formează chip-ul de citare în răspuns. Exemple: "Codul Penal", "Legea 287/2009". Vezi acte.act_citation pentru semantică completă.';

COMMENT ON COLUMN articole.link IS
'URL-ul actului-părinte pe legislatie.just.ro (preluat din acte.link). Folosit împreună cu act_citation pentru a construi link markdown în răspuns.';

COMMENT ON COLUMN articole.article_number IS
'Numărul articolului ca întreg ordinal (188 pentru "art. 188", "Art. 188", "Articolul 188"). Pentru articolele cu variantă (188 bis, 188^1), numărul de bază stă aici și sufixul în article_variant. NULL doar pentru actele nestructurate pe articole.';

COMMENT ON COLUMN articole.article_variant IS
'Sufixul de variantă al articolului, când există. Valori observate: "bis", "ter", "quater", "quinquies", "sexies", "septies", "octies" (notație latină), sau "^1", "^2", "^3" ... (notație indice). NULL pentru articolele standard. Permite distincția între "Art. 188", "Art. 188 bis" și "Art. 188^1" — articole DIFERITE introduse ulterior între numere consecutive fără renumerotare.';

COMMENT ON COLUMN articole.article_citation IS
'Citarea articolului în forma în care o scrie un jurist român, gata de afișat: "Art. 188", "Art. 188 bis", "Art. 188^1". Conține DOAR referința articolului — actul-părinte stă în act_citation. Pentru actele nestructurate conține valoarea literal "(unparsed)".';

COMMENT ON COLUMN articole.content IS
'Textul integral al articolului — toate alineatele concatenate, în ordine. Pentru regăsire mai fină pe alineat, folosește view-ul alineate.';


-- ────────────────────────────────────────────────────────────────────────────
-- ALINEATE — un rând per alineat, JOIN-uit deja cu actul-părinte
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW alineate AS
SELECT
    al.id,
    al.article_id,
    ar.act_id,
    a.act_citation,
    a.link,
    ar.article_number,
    ar.article_variant,
    ar.article_citation,
    al.paragraph_number,
    al.paragraph_citation,
    al.content
FROM read_parquet('data/laws/alineate.parquet') al
JOIN read_parquet('data/laws/articole.parquet') ar ON ar.id = al.article_id
JOIN read_parquet('data/laws/acte.parquet') a ON a.id = ar.act_id;

COMMENT ON VIEW alineate IS
'Alineatele extrase din fiecare articol. Un rând per alineat. ACEASTA ESTE UNITATEA CEA MAI FINĂ DE CITARE — corespunde cu "art. 188 alin. (1)" din practica juridică. Vine JOIN-uit deja cu actul-părinte și articolul-părinte: act_citation, link și article_citation sunt incluse direct. Pentru articolele monolitice (fără alineate distincte (1), (2), (3) ...), conține un singur rând cu paragraph_number IS NULL și content egal cu articolul întreg.';

COMMENT ON COLUMN alineate.id IS
'Cheie primară surogat.';

COMMENT ON COLUMN alineate.article_id IS
'FK către articole.id. Folosește-o când vrei să iei toate alineatele unui articol specific.';

COMMENT ON COLUMN alineate.act_id IS
'FK către acte.id (transitiv prin articole). Folosește-o pentru a restrânge la alineatele dintr-un anume act, ex: `WHERE act_id IN (SELECT id FROM cod_penal)`.';

COMMENT ON COLUMN alineate.act_citation IS
'Citarea actului-părinte. Pereche cu `link` formează chip-ul de citare în răspuns.';

COMMENT ON COLUMN alineate.link IS
'URL-ul actului-părinte pe legislatie.just.ro.';

COMMENT ON COLUMN alineate.article_number IS
'Numărul articolului-părinte (preluat din articole.article_number). Folosește-l ca să filtrezi alineatele unui articol specific: `WHERE article_number = 188`.';

COMMENT ON COLUMN alineate.article_variant IS
'Sufixul de variantă al articolului-părinte (preluat din articole.article_variant), ex: "bis", "^1". NULL pentru articolele standard.';

COMMENT ON COLUMN alineate.article_citation IS
'Citarea articolului-părinte (ex: "Art. 188"). Coloană suplimentară față de paragraph_citation, în cazul în care vrei doar referința articolului, nu și a alineatului.';

COMMENT ON COLUMN alineate.paragraph_number IS
'Numărul alineatului (1, 2, 3 ...) — exact numărul din "(1)", "(2)", "(3)". NULL = articolul nu este împărțit în alineate; conținutul rândului este articolul în întregime.';

COMMENT ON COLUMN alineate.paragraph_citation IS
'Citarea completă a alineatului în forma juridică românească: "Art. 188 alin. (1)". Include și referința articolului. Pentru articolele fără alineate (paragraph_number IS NULL), egal cu citarea articolului (ex: "Art. 188").';

COMMENT ON COLUMN alineate.content IS
'Textul alineatului, fără markeri inițiali "(N)". Pentru articolele fără alineate, textul articolului întreg.';


-- ────────────────────────────────────────────────────────────────────────────
-- VIEW-URI PE COD — selectează forma în vigoare pentru fiecare cod și pentru Constituție
-- ────────────────────────────────────────────────────────────────────────────
-- Fiecare cod român major are un view dedicat care selectează singurul rând
-- (forma în vigoare) din `acte`. Folosește-le pentru a restrânge un query la
-- nivel de articol sau alineat la un anume cod:
--   WHERE act_id IN (SELECT id FROM cod_penal)
--
-- legislatie.just.ro stochează fiecare cod consolidat sub un TipAct dedicat
-- (CODUL CIVIL, CODUL PENAL, etc.), nu sub LEGE. View-urile pe cod filtrează
-- pe TipAct + anul adoptării. Codurile cu mai multe republicări (cod proc.
-- civilă) sunt dezambiguate selectând rândul cu cel mai mult conținut.

CREATE OR REPLACE VIEW constitutie AS
SELECT * FROM acte
 WHERE type = 'CONSTITUȚIE'
   AND EXTRACT(YEAR FROM adopted_at) = 1991
   AND title ILIKE '%republicat%';

COMMENT ON VIEW constitutie IS
'Constituția României în vigoare. Forma republicată în 2003. Un singur rând. Pentru articole specifice, folosește `WHERE act_id IN (SELECT id FROM constitutie)` în view-ul articole.';


CREATE OR REPLACE VIEW cod_civil AS
SELECT * FROM acte
 WHERE type = 'CODUL CIVIL'
   AND EXTRACT(YEAR FROM adopted_at) = 2009
   AND title ILIKE '%republicat%';

COMMENT ON VIEW cod_civil IS
'Codul Civil al României în vigoare. Sursa: Legea nr. 287/2009, republicată. Reglementează raporturile civile între persoane: contracte, obligații, drepturi reale, succesiuni, familie. Un singur rând.';


CREATE OR REPLACE VIEW cod_penal AS
SELECT * FROM acte
 WHERE type = 'CODUL PENAL'
   AND EXTRACT(YEAR FROM adopted_at) = 2009;

COMMENT ON VIEW cod_penal IS
'Codul Penal al României în vigoare. Sursa: Legea nr. 286/2009. Definește infracțiunile, pedepsele și răspunderea penală. Un singur rând.';


CREATE OR REPLACE VIEW cod_muncii AS
SELECT * FROM acte
 WHERE type = 'CODUL MUNCII'
   AND EXTRACT(YEAR FROM adopted_at) = 2003
   AND title ILIKE '%republicat%';

COMMENT ON VIEW cod_muncii IS
'Codul Muncii al României în vigoare. Sursa: Legea nr. 53/2003, republicată. Reglementează raporturile individuale și colective de muncă. Un singur rând.';


CREATE OR REPLACE VIEW cod_procedura_civila AS
SELECT * FROM acte
 WHERE type = 'CODUL DE PROCEDURĂ CIVILĂ'
   AND EXTRACT(YEAR FROM adopted_at) = 2010
   AND title ILIKE '%republicat%'
 ORDER BY LENGTH(content) DESC
 LIMIT 1;

COMMENT ON VIEW cod_procedura_civila IS
'Codul de Procedură Civilă al României în vigoare. Sursa: Legea nr. 134/2010, republicată. Reglementează procedura judecății în materie civilă. Un singur rând (versiunea cu cel mai mult conținut, dintre republicări).';


CREATE OR REPLACE VIEW cod_procedura_penala AS
SELECT * FROM acte
 WHERE type = 'CODUL DE PROCEDURĂ PENALĂ'
   AND EXTRACT(YEAR FROM adopted_at) = 2010;

COMMENT ON VIEW cod_procedura_penala IS
'Codul de Procedură Penală al României în vigoare. Sursa: Legea nr. 135/2010. Reglementează procedura judecății în materie penală. Un singur rând.';


CREATE OR REPLACE VIEW cod_fiscal AS
SELECT * FROM acte
 WHERE type = 'CODUL FISCAL'
   AND EXTRACT(YEAR FROM adopted_at) = 2015;

COMMENT ON VIEW cod_fiscal IS
'Codul Fiscal al României în vigoare. Sursa: Legea nr. 227/2015. Reglementează impozitele, taxele și contribuțiile sociale. Un singur rând.';


-- ────────────────────────────────────────────────────────────────────────────
-- JURISPRUDENȚĂ
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW jurisprudenta AS
SELECT *
  FROM acte
 WHERE type IN ('DECIZIE', 'HOTĂRÂRE', 'ÎNCHEIERE', 'SENTINȚĂ')
   AND (issuer LIKE 'CURTEA CONSTITUȚIONALĂ%'
        OR issuer LIKE 'ÎNALTA CURTE DE CASAȚIE ȘI JUSTIȚIE%');

COMMENT ON VIEW jurisprudenta IS
'Hotărâri și decizii ale instanțelor supreme din România: Curtea Constituțională (CCR) și Înalta Curte de Casație și Justiție (ÎCCJ, inclusiv secțiile sale). Filtru pe type IN (DECIZIE, HOTĂRÂRE, ÎNCHEIERE, SENTINȚĂ) ȘI issuer corespunzător. NU include actele administrative ale acestor instanțe (COMUNICAT, RAPORT). Pentru paragrafele unei decizii folosește view-ul alineate cu `WHERE act_id IN (SELECT id FROM jurisprudenta WHERE ...)`.';
