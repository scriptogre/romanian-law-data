"""
Stage 1 (alt) — extracts/web.py

Fetch per-act `actiuni` HTML from legislatie.just.ro's own web endpoints
(`/Public/actiuniSuferite`, `/Public/actiuniInduse`), separate from the SOAP
API in soap.py. Output: a durable cache parquet
(act_id -> suferite_html, induse_html, fetched_at) published as a release asset,
so the slow per-act fetch runs once and later syncs fetch only the delta.

The endpoints 503 and refuse connections intermittently under load. The probe
showed the killer is the connect hang (a refused connection costs the full 30s
timeout, collapsing throughput to ~1/s), not concurrency. So: a SHORT connect
timeout (fail fast), modest concurrency, and a few retry passes over the acts
that failed - transient failures clear on a later pass.

Runs in chunks: after each chunk the cache is saved AND (in CI) re-published, so
a 6h-timeout or crash mid-run leaves a published cache to resume from. `run()`
fetches only acts NOT already cached, so a re-run continues where it stopped.
Progress (count, rate, ETA) is logged every few seconds within a chunk.

Delta (ETL_ACTIUNI_MODE=delta): new acts + a rolling reconcile slice
(oldest-fetched first) to bound staleness. NOTE: an OLD act's status changes when
ANOTHER act amends/repeals it; the full fix parses each new act's induse HTML for
its target ids and re-fetches those. That parser does not exist yet, so today's
delta = new + reconcile only.
"""

import asyncio
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from requests.adapters import HTTPAdapter

ENDPOINTS = ("actiuniSuferite", "actiuniInduse")
BASE = "https://legislatie.just.ro/Public"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

CONCURRENCY = int(os.environ.get("ETL_ACTIUNI_CONCURRENCY", 8))
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30
RETRY_PASSES = 3
CHUNK = int(os.environ.get("ETL_ACTIUNI_CHUNK", 20000))

REPO_ROOT = Path(__file__).parent.parent.parent
CORPUS_PATH = REPO_ROOT / "data" / "acte.parquet"
CACHE_PATH = REPO_ROOT / "data" / "actiuni_cache.parquet"
RECONCILE_FRACTION = float(os.environ.get("ETL_ACTIUNI_RECONCILE", 0.0333))  # ~1/30 -> ~monthly
# Set in CI to publish each checkpoint to this release tag. Unset locally = no publish.
RELEASE_TAG = os.environ.get("ETL_ACTIUNI_RELEASE_TAG")

CACHE_SCHEMA = {
    "act_id": pl.Utf8,
    "suferite_html": pl.Utf8,
    "induse_html": pl.Utf8,
    "fetched_at": pl.Datetime,
}


# --- cache -----------------------------------------------------------------

def load_cache() -> pl.DataFrame:
    return pl.read_parquet(CACHE_PATH) if CACHE_PATH.exists() else pl.DataFrame(schema=CACHE_SCHEMA)


def merge_cache(base: pl.DataFrame, new_rows: list[dict]) -> pl.DataFrame:
    """Fold freshly-fetched rows in. A re-fetched act_id overrides the old one."""
    if not new_rows:
        return base
    incoming = pl.DataFrame(new_rows, schema=CACHE_SCHEMA)
    keep = base.filter(~pl.col("act_id").is_in(incoming["act_id"]))
    return pl.concat([keep, incoming])


def ids_to_fetch(all_ids: list[str], cache: pl.DataFrame, reconcile: float) -> list[str]:
    """New (uncached) acts, plus a reconcile slice (oldest-fetched first)."""
    have = set(cache["act_id"].to_list())
    new = [i for i in all_ids if i not in have]

    stale: list[str] = []
    if reconcile > 0 and len(cache):
        n = max(1, int(len(cache) * reconcile))
        stale = cache.sort("fetched_at").head(n)["act_id"].to_list()

    return list(dict.fromkeys(new + stale))


# --- fetch -----------------------------------------------------------------

def _session() -> requests.Session:
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=64, pool_maxsize=64))
    session.headers["User-Agent"] = UA
    return session


def _fetch(session: requests.Session, endpoint: str, act_id: str) -> str | None:
    """POST one endpoint. HTML on 200, None on any failure (retried next pass)."""
    try:
        response = session.post(
            f"{BASE}/{endpoint}", data={"contor": act_id}, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        if response.status_code == 200:
            return (response.json() or {}).get("acte", "") or ""
    except Exception:
        pass
    return None


async def _one_pass(session: requests.Session, ids: list[str]) -> dict[str, tuple]:
    sem = asyncio.Semaphore(CONCURRENCY)
    result: dict[str, tuple] = {}
    start = time.time()
    last_log = start

    async def one(act_id: str) -> None:
        nonlocal last_log
        async with sem:
            suferite = await asyncio.to_thread(_fetch, session, "actiuniSuferite", act_id)
            induse = await asyncio.to_thread(_fetch, session, "actiuniInduse", act_id)
            result[act_id] = (suferite, induse)

            now = time.time()
            if now - last_log >= 15:
                last_log = now
                rate = len(result) / (now - start)
                eta_min = (len(ids) - len(result)) / rate / 60 if rate else 0
                logger.info(f"  {len(result)}/{len(ids)} acts ({rate:.0f}/s, ETA {eta_min:.0f}m)")

    await asyncio.gather(*(one(a) for a in ids))
    return result


async def fetch(ids: list[str]) -> list[dict]:
    """Fetch both endpoints for every id, retrying failures over a few passes."""
    session = _session()
    done: dict[str, tuple[str, str]] = {}
    pending = ids

    for attempt in range(1, RETRY_PASSES + 1):
        for act_id, (suferite, induse) in (await _one_pass(session, pending)).items():
            if suferite is not None and induse is not None:
                done[act_id] = (suferite, induse)
        pending = [a for a in pending if a not in done]
        logger.info(f"pass {attempt}: {len(done)}/{len(ids)} ok, {len(pending)} left")
        if not pending:
            break

    now = datetime.now(timezone.utc)
    return [
        {"act_id": a, "suferite_html": s, "induse_html": i, "fetched_at": now}
        for a, (s, i) in done.items()
    ]


# --- publish (CI only) -----------------------------------------------------

def _publish() -> None:
    """Upload the cache parquet to the RELEASE_TAG release (clobber the asset)."""
    if not RELEASE_TAG:
        return
    exists = subprocess.run(["gh", "release", "view", RELEASE_TAG], capture_output=True).returncode == 0
    if not exists:
        subprocess.run(
            ["gh", "release", "create", RELEASE_TAG, "--title", "web endpoint cache",
             "--notes", "Per-act actiuni HTML from legislatie.just.ro /Public. Rebuilt by sync-web."],
            check=True,
        )
    subprocess.run(["gh", "release", "upload", RELEASE_TAG, str(CACHE_PATH), "--clobber"], check=True)


# --- orchestration ---------------------------------------------------------

def run() -> None:
    """Fetch the actiuni delta (or full backfill) into the cache, in chunks.

    Env: ETL_ACTIUNI_MODE=backfill|delta (default delta), ETL_ACTIUNI_LIMIT=N
    (cap ids, for smoke tests). Reads the corpus from data/acte.parquet and the
    existing cache from data/actiuni_cache.parquet (both fetched by the workflow).
    Saves + (if ETL_ACTIUNI_RELEASE_TAG set) publishes after every chunk.
    """
    mode = os.environ.get("ETL_ACTIUNI_MODE", "delta")
    limit_raw = os.environ.get("ETL_ACTIUNI_LIMIT")

    corpus = pl.read_parquet(CORPUS_PATH)
    all_ids = corpus["link"].str.extract(r"(\d+)\s*$", 1).drop_nulls().to_list()
    cache = load_cache()

    reconcile = RECONCILE_FRACTION if mode == "delta" else 0.0
    ids = ids_to_fetch(all_ids, cache, reconcile)
    if limit_raw:
        ids = ids[: int(limit_raw)]
    logger.info(f"actiuni {mode}: {len(ids)} to fetch (corpus={len(all_ids)}, cached={len(cache)})")

    for start in range(0, len(ids), CHUNK):
        chunk = ids[start : start + CHUNK]
        rows = asyncio.run(fetch(chunk))
        cache = merge_cache(cache, rows)
        cache.write_parquet(CACHE_PATH, compression="zstd")
        _publish()
        logger.success(f"checkpoint {min(start + CHUNK, len(ids))}/{len(ids)} done, cache={len(cache)} acts")

    logger.success(f"actiuni {mode} done: cache={len(cache)} acts -> {CACHE_PATH}")
