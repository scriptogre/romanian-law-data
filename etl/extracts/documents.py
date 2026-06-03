"""
Stage 1 — extracts/documents.py

Fetch every act from `legislatie.just.ro` by sweeping the document-id space and
reading each act's public detail page (`/Public/DetaliiDocument/{id}`). Output:
parquet parts under `data/raw_documents/`, the same 8-field schema the transform
expects. Stage 2 (transform) reads them as a glob.

WHY NOT SOAP. The SOAP `Search` API has no stable sort: the same (page, token)
returns different records on different requests (the server pages over a handful
of out-of-order replicas). Paginating, especially strided across shards/IPs,
therefore cannot enumerate the archive. A page walk lands ~67% of acts and drops
a random third. Measured: a fixed 20-page window kept yielding new ids across
repeated passes (197 -> 247 -> 297 -> ... ) instead of saturating. So we stopped
trusting pagination order and index by id instead, which IS dense and stable:
LinkHtml always ends in `{id}`, ids run ~1..MAX with only scattered dead ones,
and `DetaliiDocument/{id}` returns any single act deterministically.

SHARDED across runners (= IPs): the id space is split by stride, shard `s` of `N`
takes ids s+1, s+1+N, s+1+2N, ... Each runner writes its own part files (named by
shard + chunk, no collisions); the publish job reads them all. The site runs a
per-IP token bucket (~1 req/s after a few hundred, 503 under load), so N runners
~= Nx throughput. Throttle (503) and transient errors are retried with backoff;
dead ids (404 / 500 / a page with no act header) are skipped, not retried.
"""

import asyncio
import html as html_lib
import os
import re
import time
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from requests.adapters import HTTPAdapter

from etl.extracts import versions

BASE = "https://legislatie.just.ro/Public"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# SOAP record fields, kept verbatim so the transform is unchanged. The detail
# page carries all of them: id (LinkHtml), title + subtitle (S_DEN/S_PAR),
# issuer (S_EMT_BDY), publication (S_PUB_BDY), and the full act body.
SOAP_FIELDS = ("Titlu", "Text", "TipAct", "Numar", "Emitent", "Publicatie", "DataVigoare", "LinkHtml")

def _env_int(name: str, default: int) -> int:
    """Read an int env var, treating unset OR empty (CI passes "" for blank inputs) as the default."""
    return int(os.environ.get(name) or default)


CONCURRENCY = _env_int("ETL_DOCUMENTS_CONCURRENCY", 8)
CHUNK = _env_int("ETL_DOCUMENTS_CHUNK", 5000)
MIN_ID = _env_int("ETL_DOCUMENTS_MIN_ID", 1)
# Top of the id sweep = the newest document id on the portal. Discovered at run time
# (see discover_max_id); the env var pins it for one run, the constant is only a
# last-resort fallback if probing fails.
MAX_ID_FALLBACK = 320000
# How the probe finds the newest id: the corpus is dense at the top (consecutive ids,
# gaps of 0-1) and hard-dead above the newest act, so a run of END_OF_CORPUS_DEAD_RUN
# consecutive dead ids reliably means we've passed the end. Walk up from PROBE_START_ID,
# then sweep SWEEP_MARGIN past the newest act so acts published mid-run are still caught.
# PROBE_ABORT_ID is a runaway guard.
PROBE_START_ID = 1024
END_OF_CORPUS_DEAD_RUN = 64
SWEEP_MARGIN = 1000
PROBE_ABORT_ID = 5_000_000
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30
RETRY_PASSES = 4
THROTTLE_BACKOFF = 2.0  # seconds to wait after a 503/429 before the id is retried next pass
# 500 is the portal's "no act at this id" signal (every dead id returns it) but also a
# possible transient error. Re-check a few times inline: a real act recovers, a dead id
# stays 500. Without this a real act that 500s once under load would be silently dropped.
SOFT_RETRIES = 2
SOFT_RETRY_BACKOFF = 2.0

REPO_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw_documents"

RO_MONTHS = {
    "ianuarie": 1, "februarie": 2, "martie": 3, "aprilie": 4, "mai": 5, "iunie": 6,
    "iulie": 7, "august": 8, "septembrie": 9, "octombrie": 10, "noiembrie": 11, "decembrie": 12,
}


def _clean(fragment: str) -> str:
    """HTML fragment -> readable text: drop scripts/styles, turn blocks into newlines, unescape."""
    fragment = re.sub(r"<script.*?</script>", " ", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<style.*?</style>", " ", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"</(p|div|tr|li|h[1-6]|table)>", "\n", fragment, flags=re.I)
    fragment = re.sub(r"<[^>]+>", " ", fragment)
    fragment = html_lib.unescape(fragment).replace("\xa0", " ")
    fragment = re.sub(r"[ \t]+", " ", fragment)
    fragment = re.sub(r" *\n *", "\n", fragment)
    fragment = re.sub(r"\n{3,}", "\n\n", fragment)
    return fragment.strip()


def _span(html: str, css_class: str) -> str | None:
    match = re.search(rf'<span class="{css_class}"[^>]*>(.*?)</span>', html, re.S)
    return _clean(match.group(1)) if match else None


def _publication_date_iso(publicatie: str | None) -> str | None:
    """Effective date proxy: the Monitorul Oficial publication date (acts enter force on publication)."""
    if not publicatie:
        return None
    match = re.search(r"(\d{1,2})\s+([a-zăâîşșţț]+)\s+(\d{4})", publicatie, re.I)
    if not match:
        return None
    day, month_name, year = match.group(1), match.group(2).lower(), match.group(3)
    month_name = month_name.replace("ş", "s").replace("ș", "s").replace("ţ", "t").replace("ț", "t")
    months = {k.replace("ş", "s").replace("ț", "t"): v for k, v in RO_MONTHS.items()}
    month = months.get(month_name)
    return f"{int(year):04d}-{month:02d}-{int(day):02d}" if month else None


def parse_document(document_id: int, html: str) -> dict | None:
    """Parse one detail page into the 8-field record. None if the page holds no act."""
    if 'class="S_DEN"' not in html:
        return None

    den = _span(html, "S_DEN") or ""
    par = _span(html, "S_PAR") or ""
    titlu = f"{den} {par}".strip() or None

    head = re.match(r"\s*([A-ZĂÂÎŞȘŢȚ.]+(?:\s+[A-ZĂÂÎŞȘŢȚ.]+)?)\s+nr\.?\s*([\d.]+)", den)
    tip_act = head.group(1).strip() if head else (den.split()[0] if den else None)
    numar = head.group(2).strip(".") if head else None

    emitent_match = re.search(r'<span class="S_EMT_BDY">(.*?)</span>', html, re.S)
    emitent = _clean(emitent_match.group(1)) if emitent_match else None

    publicatie = _span(html, "S_PUB_BDY")
    data_vigoare = _publication_date_iso(publicatie)

    start = html.find('<span class="S_DEN">')
    end = next(
        (i for i in (html.find(m, start) for m in ('id="fisa_act_container"', 'data-id="FisaAct"')) if i != -1),
        len(html),
    )
    text = _clean(html[start:end])

    return {
        "Titlu": titlu,
        "Text": text,
        "TipAct": tip_act,
        "Numar": numar,
        "Emitent": emitent,
        "Publicatie": publicatie,
        "DataVigoare": data_vigoare,
        "LinkHtml": f"{BASE}/DetaliiDocument/{document_id}",
    }


def _session() -> requests.Session:
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=64, pool_maxsize=64))
    session.headers["User-Agent"] = UA
    return session


def fetch_document(session: requests.Session, document_id: int) -> tuple | None | str:
    """
    Fetch + parse one act. Returns:
      (record, version_rows) -> the act record plus its consolidation timeline
      None  -> dead id (no act here): a 200 page with no act header, or a 500 that persists
      "retry" -> transient (throttle / timeout / connection): try again next pass
    """
    for attempt in range(SOFT_RETRIES + 1):
        try:
            response = session.get(
                f"{BASE}/DetaliiDocument/{document_id}", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )
        except Exception:
            return "retry"

        code = response.status_code
        if code == 200:
            record = parse_document(document_id, response.text)
            if record is None:
                return None
            return record, versions.parse_versions(document_id, response.text)
        if code in (429, 503):
            time.sleep(THROTTLE_BACKOFF)
            return "retry"
        if code == 500:  # dead id OR a transient error; re-check before giving up
            if attempt < SOFT_RETRIES:
                time.sleep(SOFT_RETRY_BACKOFF)
                continue
            return None
        return "retry"
    return None


async def _one_pass(session: requests.Session, ids: list[int]) -> dict[int, tuple | None | str]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    result: dict[int, tuple | None | str] = {}
    start = time.time()
    last_log = start

    async def one(document_id: int) -> None:
        nonlocal last_log
        async with semaphore:
            result[document_id] = await asyncio.to_thread(fetch_document, session, document_id)
            now = time.time()
            if now - last_log >= 15:
                last_log = now
                acts = sum(1 for r in result.values() if isinstance(r, tuple))
                rate = len(result) / (now - start)
                eta_min = (len(ids) - len(result)) / rate / 60 if rate else 0
                logger.info(f"  {len(result)}/{len(ids)} fetched, {acts} acts ({rate:.1f}/s, ETA {eta_min:.0f}m)")

    await asyncio.gather(*(one(i) for i in ids))
    return result


async def fetch_chunk(session: requests.Session, ids: list[int]) -> list[tuple]:
    """Fetch a chunk of ids, retrying only the transient ones over a few passes.

    Each kept item is `(record, version_rows)` parsed from one act page.
    """
    acts: dict[int, tuple] = {}
    pending = ids

    for attempt in range(1, RETRY_PASSES + 1):
        outcomes = await _one_pass(session, pending)
        retry = []
        for document_id, outcome in outcomes.items():
            if isinstance(outcome, tuple):
                acts[document_id] = outcome
            elif outcome == "retry":
                retry.append(document_id)
            # None -> dead id, drop it
        pending = retry
        logger.info(f"  pass {attempt}: {len(acts)} acts, {len(pending)} transient left")
        if not pending:
            break

    if pending:
        logger.warning(f"  {len(pending)} ids still failing after {RETRY_PASSES} passes (likely throttle)")
    return [acts[i] for i in sorted(acts)]


def _is_live(session: requests.Session, document_id: int) -> bool | None:
    """True = a real act here, False = dead id (the portal's 500), None = transient (retry)."""
    try:
        response = session.get(
            f"{BASE}/DetaliiDocument/{document_id}", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
    except Exception:
        return None
    if response.status_code == 200:
        return 'class="S_DEN"' in response.text
    if response.status_code == 500:  # the portal's "no act at this id" signal
        return False
    return None  # 429 / 503 / anything else -> transient


def _has_live_act_in(session: requests.Session, start: int, count: int) -> bool:
    """True if any id in [start, start+count) is a real act. A persistently transient id
    counts as live, so throttling can never fake the end of the corpus (we'd rather
    overshoot the newest id and sweep some cheap dead ids than stop short and lose acts)."""
    for document_id in range(start, start + count):
        live = _is_live(session, document_id)
        for _ in range(SOFT_RETRIES):
            if live is not None:
                break
            time.sleep(THROTTLE_BACKOFF)
            live = _is_live(session, document_id)
        if live is None or live:  # a real act, or gave up while throttled -> treat as live
            return True
    return False


def discover_max_id(session: requests.Session) -> int:
    """Find the newest document id on the portal and return it plus a margin, with no
    hardcoded ceiling. This is the top of the id sweep.

    Walk up from PROBE_START_ID, doubling, until a run of END_OF_CORPUS_DEAD_RUN dead ids
    shows we've passed the newest act; binary-search the exact end; add SWEEP_MARGIN so
    acts published mid-run are still caught. Falls back to MAX_ID_FALLBACK only if the
    probe runs away past PROBE_ABORT_ID.
    """
    last_live, first_dead = PROBE_START_ID, PROBE_START_ID
    while _has_live_act_in(session, first_dead, END_OF_CORPUS_DEAD_RUN):
        last_live, first_dead = first_dead, first_dead * 2
        if first_dead > PROBE_ABORT_ID:
            logger.warning(f"id probe exceeded {PROBE_ABORT_ID}; using fallback {MAX_ID_FALLBACK}")
            return MAX_ID_FALLBACK

    while first_dead - last_live > 1:
        mid = (last_live + first_dead) // 2
        if _has_live_act_in(session, mid, END_OF_CORPUS_DEAD_RUN):
            last_live = mid
        else:
            first_dead = mid

    max_id = last_live + END_OF_CORPUS_DEAD_RUN + SWEEP_MARGIN
    logger.success(f"newest document id is near {last_live}; sweeping through {max_id}")
    return max_id


def print_max_id() -> None:
    """CLI entry (stage `documents-max-id`): print the discovered newest id to stdout so
    the prepare job can hand one ETL_DOCUMENTS_MAX_ID to every shard. Logs go to stderr."""
    print(discover_max_id(_session()))


def run() -> None:
    """Sweep this shard's id stride, fetching detail pages in chunks into parquet parts."""
    concurrency = CONCURRENCY
    shard = int(os.environ.get("ETL_DOCUMENTS_SHARD", 0))
    shards = int(os.environ.get("ETL_DOCUMENTS_SHARDS", 1))
    limit_raw = os.environ.get("ETL_DOCUMENTS_LIMIT")
    limit = int(limit_raw) if limit_raw else None

    session = _session()

    # Ceiling: pinned by env (the prepare job discovers it once and hands it to every
    # shard), else discover it here so a standalone local run still sweeps the whole corpus.
    max_id_env = os.environ.get("ETL_DOCUMENTS_MAX_ID")
    max_id = int(max_id_env) if max_id_env else discover_max_id(session)

    schema = {field: pl.Utf8 for field in SOAP_FIELDS}
    ids = list(range(MIN_ID + shard, max_id + 1, shards))
    if limit is not None:
        ids = ids[:limit]

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(
        f"documents: shard {shard}/{shards}, ids {MIN_ID}..{max_id} (stride {shards}, "
        f"{len(ids)} this shard), concurrency {concurrency}"
    )

    total = 0
    for offset in range(0, len(ids), CHUNK):
        chunk = ids[offset : offset + CHUNK]
        results = asyncio.run(fetch_chunk(session, chunk))
        if results:
            records = [record for record, _ in results]
            version_rows = [row for _, rows in results for row in rows]

            part = RAW_DIR / f"part_shard{shard:03d}_{chunk[0]:08d}.parquet"
            pl.DataFrame(records, schema=schema).write_parquet(part, compression="zstd")
            if version_rows:
                versions.write_part(version_rows, shard, chunk[0])

            total += len(records)
            logger.success(
                f"shard {shard}: +{len(records)} acts, {len(version_rows)} versions "
                f"(total {total}, through id {chunk[-1]}) -> {part.name}"
            )

    logger.success(f"shard {shard}/{shards} done: {total} acts, swept {len(ids)} ids")
