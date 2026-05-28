-- ============================================================================
-- LLM-facing surface for the Romanian legal corpus.
--
-- Table names use Romanian legal vocabulary (acte, articole, alineate).
-- Column names stay in English SQL convention. Every column has a Romanian
-- COMMENT ON explaining what it contains, what the expected format is, and
-- when it's NULL.
--
-- Subject lenses (constitutie, cod_civil, cod_penal, ...) are filters on
-- `acte` that select the single act that IS the code or constitution.
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- BASE VIEWS
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW acte AS
SELECT
    id,
    type,
    number,
    canonical_citation,
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
'Acte normative din corpusul juridic român. Sursa: legislatie.just.ro (Ministerul Justiției). Un rând per act distinct. Acoperă LEGI, ORDONANȚE (OUG, OG), HOTĂRÂRI DE GUVERN (HG), ORDINE ministeriale, DECRETE prezidențiale, DECIZII și HOTĂRÂRI ale Curții Constituționale (CCR) și ÎCCJ, precum și documente conexe (RAPORT, COMUNICAT, RECTIFICARE, CUANTUM TOTAL). Pentru regăsire la nivel de articol sau alineat, fă JOIN cu articole sau alineate pe id ↔ act_id.';

COMMENT ON COLUMN acte.id IS
'Identificator unic, sintetic, generat de pipeline. Cheie primară. FK către articole.act_id.';

COMMENT ON COLUMN acte.type IS
'Tipul actului, exact cum este clasificat în Monitorul Oficial. Valori frecvente: LEGE, ORDONANȚĂ DE URGENȚĂ, ORDONANȚĂ, HOTĂRÂRE, ORDIN, DECRET, DECIZIE, ÎNCHEIERE, SENTINȚĂ, COMUNICAT, RAPORT, RECTIFICARE, CUANTUM TOTAL, NORMĂ, METODOLOGIE, REGULAMENT, INSTRUCȚIUNI, CIRCULARĂ, ANEXĂ, CONSTITUȚIE. Acesta este principalul câmp pentru filtrarea după natura documentului — folosește-l direct în WHERE.';

COMMENT ON COLUMN acte.number IS
'Numărul oficial al actului EXACT cum este returnat de SOAP: de obicei doar numărul ("287", "75"), uneori "număr/an" ("286/2009"). NULL pentru documente fără număr distinct (COMUNICAT-uri ÎCCJ, RAPORT-uri, CUANTUM TOTAL, RECTIFICARI, și — important — pentru CODURI și CONSTITUȚIE, care sunt stocate cu type = "CODUL CIVIL" / "CODUL PENAL" / "CONSTITUȚIE" și number IS NULL, chiar dacă în vorbire sunt citate ca "Legea 287/2009" etc). PENTRU CĂUTARE DUPĂ CITAREA OBIȘNUITĂ ("Legea 287/2009", "OUG 100/2024", "Codul Civil") FOLOSEȘTE `canonical_citation`, NU `number`.';

COMMENT ON COLUMN acte.canonical_citation IS
'Citarea canonică, în forma pe care o folosesc juriștii români: "Legea 287/2009", "OUG 100/2024", "HG 405/2026", "Ordinul 744/2026", "Decretul 251/2026", "Decizia 175/2025", "Codul Civil", "Codul Penal", "Constituția României". Calculată din (type, number, adopted_at, issuer) la ETL. ACEASTA ESTE COLOANA DE FOLOSIT pentru lookup direct după o referință legală cunoscută — ex: `WHERE canonical_citation = ''Legea 287/2009''`. Pentru acte fără număr, conține "{tip} din YYYY-MM-DD" (ex: "Comunicatul din 2025-04-15"). Pentru coduri și Constituție conține numele canonic (mai multe republicări împart aceeași citation — folosește view-urile dedicate `cod_civil` / `constitutie` / ... pentru forma în vigoare).';

COMMENT ON COLUMN acte.issuer IS
'Autoritatea emitentă, în majuscule, exact cum apare în antetul actului din Monitorul Oficial. Exemple: "PARLAMENTUL ROMÂNIEI" (pentru legi), "GUVERNUL ROMÂNIEI" (pentru OUG, OG, HG), "MINISTERUL JUSTIȚIEI", "CURTEA CONSTITUȚIONALĂ" (pentru decizii CCR), "ÎNALTA CURTE DE CASAȚIE ȘI JUSTIȚIE" (pentru decizii ÎCCJ), "PREȘEDINTELE ROMÂNIEI" (pentru decrete). Pentru ordine comune (joint orders), conține toți emitenții separați prin " / " — ex: "MINISTERUL FINANȚELOR / MINISTERUL DEZVOLTĂRII".';

COMMENT ON COLUMN acte.title IS
'Titlul oficial al actului, curățat de antetul tehnic ("EMITENT ... PUBLICAT ÎN ...") care apare în răspunsul SOAP. Conține de obicei tipul, numărul, data adoptării și obiectul reglementării. Exemplu: "LEGE nr. 75 din 21 mai 2026 pentru modificarea art. 597 din Legea nr. 135/2010 privind Codul de procedură penală".';

COMMENT ON COLUMN acte.content IS
'Textul integral al actului, în format text simplu (plain text). Câmp mare — poate depăși 100 KB pentru acte voluminoase (coduri, anexe extinse). Pentru regăsire structurată pe articol sau alineat, NU folosi acest câmp; folosește views-urile articole și alineate, care conțin aceleași informații parsate ierarhic.';

COMMENT ON COLUMN acte.adopted_at IS
'Data la care actul a fost adoptat / semnat, extrasă din titlu (ex: "LEGE nr. 75 din 21 mai 2026" → 2026-05-21). Pentru legi: data votului final în Parlament. Pentru OUG / HG: data ședinței de Guvern. Pentru ordine ministeriale: data semnării de ministru. Pentru decizii CCR / ÎCCJ: data pronunțării. NULL dacă tiparul nu a putut fi extras din titlu.';

COMMENT ON COLUMN acte.published_at IS
'Data publicării actului în Monitorul Oficial al României, Partea I. ACEASTA ESTE DATA FOLOSITĂ CANONIC PENTRU CITAREA LEGALĂ în România (ex: "Legea nr. 75/2026 publicată în M.Of. nr. 431 din 21 mai 2026"). Extrasă din antetul textului ("Publicat în MONITORUL OFICIAL nr. N din DD luna YYYY"). De obicei coincide cu adopted_at sau este la 1-7 zile mai târziu. NULL pentru documente nepublicate în Monitorul Oficial — în special COMUNICATE-le ÎCCJ, care apar doar pe www.scj.ro (la fel pentru gazette_number).';

COMMENT ON COLUMN acte.effective_at IS
'Data intrării în vigoare. Conform articolului 78 din Constituția României, legile intră în vigoare la 3 zile de la publicarea în Monitorul Oficial, dacă nu specifică altă dată. HOTĂRÂRILE DE GUVERN cu caracter individual (numiri, sancțiuni, autorizații) intră în vigoare imediat la publicare. DECIZIILE CCR sunt obligatorii de la publicarea în MO. Sursa: câmpul DataVigoare din SOAP-ul legislatie.just.ro. Acest câmp răspunde la întrebarea "este actul în vigoare?".';

COMMENT ON COLUMN acte.gazette_number IS
'Numărul de Monitor Oficial al României, Partea I, în care a fost publicat actul (ex: "M.Of. nr. 431" → 431). Folosit împreună cu published_at pentru citarea canonică completă: "publicat în M.Of. nr. 431 din 21 mai 2026". NULL dacă tiparul nu a putut fi extras din antetul textului.';

COMMENT ON COLUMN acte.link IS
'URL absolut către pagina de detaliu a actului pe legislatie.just.ro. Format: http://legislatie.just.ro/Public/DetaliiDocument/{id}.';


CREATE OR REPLACE VIEW articole AS
SELECT
    id,
    act_id,
    number,
    number_variant,
    full_path,
    content
FROM read_parquet('data/laws/articole.parquet');

COMMENT ON VIEW articole IS
'Articolele extrase din fiecare act. Un rând per articol. Pentru actele care nu sunt structurate pe articole (decizii CCR/ÎCCJ, comunicate, rapoarte), un singur rând cu number IS NULL și content egal cu textul întreg al actului. Pentru regăsire la nivel de alineat, fă JOIN cu alineate pe id ↔ article_id.';

COMMENT ON COLUMN articole.id IS
'Identificator unic, sintetic, generat de pipeline. Cheie primară. FK către alineate.article_id.';

COMMENT ON COLUMN articole.act_id IS
'FK către acte.id. Folosește JOIN pe acte pentru a obține metadatele actului-părinte (tip, număr, emitent, date).';

COMMENT ON COLUMN articole.number IS
'Numărul articolului ca întreg ordinal (188 pentru "art. 188", "Art. 188", "Articolul 188"). Pentru articole cu variantă (188 bis, 188^1) numărul de bază este aici; sufixul este în number_variant. NULL doar pentru actele nestructurate pe articole.';

COMMENT ON COLUMN articole.number_variant IS
'Sufixul de variantă al numărului de articol, când există. Valori observate: "bis", "ter", "quater", "quinquies", "sexies", "septies", "octies" (notație latină), sau "^1", "^2", "^3" ... (notație indice). NULL pentru articolele standard. Acest câmp permite distincția între "Art. 188" și "Art. 188 bis" sau "Art. 188^1", care sunt articole DIFERITE introduse ulterior între numerele standard fără renumerotare.';

COMMENT ON COLUMN articole.full_path IS
'Citarea umană a articolului, formatată gata de afișat: "Art. 188", "Art. 188 bis", "Art. 188^1". Pentru actele nestructurate, conține valoarea literal "(unparsed)".';

COMMENT ON COLUMN articole.content IS
'Textul integral al articolului — toate alineatele concatenate, în ordine. Pentru o regăsire mai fină pe alineat, folosește alineate.';


CREATE OR REPLACE VIEW alineate AS
SELECT
    id,
    article_id,
    number,
    full_path,
    content
FROM read_parquet('data/laws/alineate.parquet');

COMMENT ON VIEW alineate IS
'Alineatele extrase din fiecare articol. Un rând per alineat. ACEASTA ESTE UNITATEA CEA MAI FINĂ DE CITARE — corespunde cu "art. 188 alin. (1)" din practica juridică românească. Pentru articole monolitice (fără alineate distincte (1), (2), (3) ...), conține un singur rând cu number IS NULL și content egal cu articolul întreg.';

COMMENT ON COLUMN alineate.id IS
'Identificator unic, sintetic, generat de pipeline.';

COMMENT ON COLUMN alineate.article_id IS
'FK către articole.id. Folosește JOIN pe articole → acte pentru a obține contextul complet (articolul-părinte și actul-părinte).';

COMMENT ON COLUMN alineate.number IS
'Numărul alineatului (1, 2, 3 ...) — exact numărul din "(1)", "(2)", "(3)". NULL = articolul nu este împărțit în alineate; conținutul rândului este articolul în întregime.';

COMMENT ON COLUMN alineate.full_path IS
'Citarea umană completă: "Art. 188 alin. (1)". Pentru articolele fără alineate (number IS NULL), egal cu full_path-ul articolului-părinte (ex: "Art. 188").';

COMMENT ON COLUMN alineate.content IS
'Textul alineatului, fără markeri inițiali "(N)". Pentru articolele fără alineate, textul articolului întreg.';


-- ────────────────────────────────────────────────────────────────────────────
-- SUBJECT LENSES — un view per cod
-- ────────────────────────────────────────────────────────────────────────────
-- Fiecare cod român major are un view dedicat care selectează singurul act
-- (singura LEGE) care ESTE codul respectiv, identificat prin numărul ei
-- canonic. Un cod = un singur rând în view. Pentru articole / alineate,
-- fă JOIN cu articole / alineate pe id ↔ act_id.

-- legislatie.just.ro stocheaza fiecare cod consolidat sub un TipAct dedicat
-- (CODUL CIVIL, CODUL PENAL, etc.), nu sub LEGE. View-urile pe cod filtrează
-- pe TipAct + anul adoptării. Codurile cu mai multe republicări (cod proc.
-- civilă) sunt dezambiguate selectând rândul cu cel mai mult conținut.

CREATE OR REPLACE VIEW constitutie AS
SELECT * FROM acte
 WHERE type = 'CONSTITUȚIE'
   AND EXTRACT(YEAR FROM adopted_at) = 1991
   AND title ILIKE '%republicat%';

COMMENT ON VIEW constitutie IS
'Constituția României în vigoare. Filtru: type = ''CONSTITUȚIE'', adoptată în 1991, forma republicată în 2003. Un singur rând. Pentru articole specifice, fă JOIN cu articole pe acte.id = articole.act_id.';


CREATE OR REPLACE VIEW cod_civil AS
SELECT * FROM acte
 WHERE type = 'CODUL CIVIL'
   AND EXTRACT(YEAR FROM adopted_at) = 2009
   AND title ILIKE '%republicat%';

COMMENT ON VIEW cod_civil IS
'Codul Civil al României. Sursa: Legea nr. 287/2009 (M.Of. nr. 511 din 24 iulie 2009, republicată). Reglementează raporturile civile între persoane: contracte, obligații, drepturi reale, succesiuni, familie. Un singur rând (actul-cod). Pentru articole specifice, fă JOIN cu articole pe acte.id = articole.act_id.';


CREATE OR REPLACE VIEW cod_penal AS
SELECT * FROM acte
 WHERE type = 'CODUL PENAL'
   AND EXTRACT(YEAR FROM adopted_at) = 2009;

COMMENT ON VIEW cod_penal IS
'Codul Penal al României. Sursa: Legea nr. 286/2009 (M.Of. nr. 510 din 24 iulie 2009). Definește infracțiunile, pedepsele și răspunderea penală. Un singur rând. Pentru articole specifice (ex: art. 188 — omorul), fă JOIN cu articole pe acte.id = articole.act_id.';


CREATE OR REPLACE VIEW cod_muncii AS
SELECT * FROM acte
 WHERE type = 'CODUL MUNCII'
   AND EXTRACT(YEAR FROM adopted_at) = 2003
   AND title ILIKE '%republicat%';

COMMENT ON VIEW cod_muncii IS
'Codul Muncii al României. Sursa: Legea nr. 53/2003 (M.Of. nr. 72 din 5 februarie 2003, republicată). Reglementează raporturile individuale și colective de muncă. Un singur rând. Pentru articole specifice, fă JOIN cu articole pe acte.id = articole.act_id.';


CREATE OR REPLACE VIEW cod_procedura_civila AS
SELECT * FROM acte
 WHERE type = 'CODUL DE PROCEDURĂ CIVILĂ'
   AND EXTRACT(YEAR FROM adopted_at) = 2010
   AND title ILIKE '%republicat%'
 ORDER BY LENGTH(content) DESC
 LIMIT 1;

COMMENT ON VIEW cod_procedura_civila IS
'Codul de Procedură Civilă al României. Sursa: Legea nr. 134/2010 (M.Of. nr. 485 din 15 iulie 2010, republicată). Reglementează procedura judecății în materie civilă. Un singur rând. Pentru articole specifice, fă JOIN cu articole pe acte.id = articole.act_id.';


CREATE OR REPLACE VIEW cod_procedura_penala AS
SELECT * FROM acte
 WHERE type = 'CODUL DE PROCEDURĂ PENALĂ'
   AND EXTRACT(YEAR FROM adopted_at) = 2010;

COMMENT ON VIEW cod_procedura_penala IS
'Codul de Procedură Penală al României. Sursa: Legea nr. 135/2010 (M.Of. nr. 486 din 15 iulie 2010). Reglementează procedura judecății în materie penală. Un singur rând. Pentru articole specifice, fă JOIN cu articole pe acte.id = articole.act_id.';


CREATE OR REPLACE VIEW cod_fiscal AS
SELECT * FROM acte
 WHERE type = 'CODUL FISCAL'
   AND EXTRACT(YEAR FROM adopted_at) = 2015;

COMMENT ON VIEW cod_fiscal IS
'Codul Fiscal al României. Sursa: Legea nr. 227/2015 (M.Of. nr. 688 din 10 septembrie 2015). Reglementează impozitele, taxele și contribuțiile sociale. Un singur rând. Pentru articole specifice, fă JOIN cu articole pe acte.id = articole.act_id.';


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
'Hotărâri și decizii ale instanțelor supreme din România: Curtea Constituțională (CCR) și Înalta Curte de Casație și Justiție (ÎCCJ, inclusiv secțiile sale). Filtru pe type IN (DECIZIE, HOTĂRÂRE, ÎNCHEIERE, SENTINȚĂ) ȘI issuer corespunzător. NU include actele administrative ale acestor instanțe (COMUNICAT, RAPORT — pentru acelea, folosește acte cu filtru explicit pe type și issuer).';
