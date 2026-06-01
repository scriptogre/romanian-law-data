"""
Stage 1 (alt) — extracts/web.py

Fetch per-act `actiuni` HTML from legislatie.just.ro's own web endpoints
(`/Public/actiuniSuferite`, `/Public/actiuniInduse`), separate from the SOAP
API in soap.py. Output: a durable cache parquet
(act_id -> suferite_html, induse_html, fetched_at) published as a release asset.

THROTTLE: the site runs a per-IP token bucket - a few hundred acts fetch fast,
then that IP is choked to ~1 req/s (connections start timing out). One IP cannot
do the backfill. So the work is SHARDED across many runners (= many IPs): each
runner fetches its slice (ids[shard::shards]) and writes a shard parquet; a
separate merge step folds the shards into the cache and publishes. Resumable:
`run()` fetches only acts NOT already cached, so re-dispatching reshards whatever
is still missing until the cache is full.

Within a runner: SHORT connect timeout (a refused connection fails in 5s, not
30s), modest concurrency, a few retry passes. Progress (ok/processed, rate, ETA)
logs every few seconds, and the shard parquet is checkpointed to disk per chunk.

Env:
  ETL_ACTIUNI_MODE      backfill | delta (default delta)
  ETL_ACTIUNI_SHARD     this runner's index, 0-based (default 0)
  ETL_ACTIUNI_SHARDS    total runners (default 1)
  ETL_ACTIUNI_LIMIT     cap ids (smoke test)
  ETL_ACTIUNI_RELEASE_TAG  merge stage publishes the cache to this tag (CI only)
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
CHUNK = int(os.environ.get("ETL_ACTIUNI_CHUNK", 5000))

REPO_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = REPO_ROOT / "data"
CORPUS_PATH = DATA_DIR / "acte.parquet"
CACHE_PATH = DATA_DIR / "actiuni_cache.parquet"
RECONCILE_FRACTION = float(os.environ.get("ETL_ACTIUNI_RECONCILE", 0.0333))  # ~1/30 -> ~monthly
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
                ok = sum(1 for s, i in result.values() if s is not None and i is not None)
                rate = len(result) / (now - start)
                eta_min = (len(ids) - len(result)) / rate / 60 if rate else 0
                logger.info(f"  {len(result)}/{len(ids)} done, {ok} ok ({rate:.1f}/s, ETA {eta_min:.0f}m)")

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


# --- orchestration ---------------------------------------------------------

def _shard_path(shard: int) -> Path:
    return DATA_DIR / f"actiuni_shard_{shard}.parquet"


def run() -> None:
    """Fetch this shard's uncached acts, in chunks, into its shard parquet.

    Does NOT publish or touch the cache; the merge stage folds shards in. Sharded
    so many runners (= many IPs) split the work past the per-IP throttle.
    """
    mode = os.environ.get("ETL_ACTIUNI_MODE", "delta")
    shard = int(os.environ.get("ETL_ACTIUNI_SHARD", 0))
    shards = int(os.environ.get("ETL_ACTIUNI_SHARDS", 1))
    limit_raw = os.environ.get("ETL_ACTIUNI_LIMIT")

    corpus = pl.read_parquet(CORPUS_PATH)
    all_ids = corpus["link"].str.extract(r"(\d+)\s*$", 1).drop_nulls().to_list()
    cache = load_cache()

    reconcile = RECONCILE_FRACTION if mode == "delta" else 0.0
    ids = ids_to_fetch(all_ids, cache, reconcile)[shard::shards]
    if limit_raw:
        ids = ids[: int(limit_raw)]
    logger.info(f"actiuni {mode} shard {shard}/{shards}: {len(ids)} to fetch (uncached total slice)")

    out = _shard_path(shard)
    rows: list[dict] = []
    for start in range(0, len(ids), CHUNK):
        rows += asyncio.run(fetch(ids[start : start + CHUNK]))
        pl.DataFrame(rows, schema=CACHE_SCHEMA).write_parquet(out, compression="zstd")
        logger.success(f"shard {shard}: {len(rows)} ok / {min(start + CHUNK, len(ids))} processed -> {out.name}")

    logger.success(f"shard {shard} done: {len(rows)} acts fetched")


def merge() -> None:
    """Fold all shard parquets into the cache and publish it."""
    cache = load_cache()
    shard_files = sorted(DATA_DIR.glob("actiuni_shard_*.parquet"))
    rows = (
        pl.concat([pl.read_parquet(f) for f in shard_files]).to_dicts() if shard_files else []
    )
    merged = merge_cache(cache, rows)
    merged.write_parquet(CACHE_PATH, compression="zstd")
    logger.success(f"merge: cache {len(cache)} -> {len(merged)} (+{len(rows)} from {len(shard_files)} shards)")

    corpus = pl.read_parquet(CORPUS_PATH)
    all_ids = corpus["link"].str.extract(r"(\d+)\s*$", 1).drop_nulls().to_list()
    remaining = len(set(all_ids) - set(merged["act_id"].to_list()))
    logger.info(f"REMAINING={remaining}")

    if RELEASE_TAG:
        exists = subprocess.run(["gh", "release", "view", RELEASE_TAG], capture_output=True).returncode == 0
        if not exists:
            subprocess.run(
                ["gh", "release", "create", RELEASE_TAG, "--title", "web endpoint cache",
                 "--notes", "Per-act actiuni HTML from legislatie.just.ro /Public. Rebuilt by sync-web."],
                check=True,
            )
        subprocess.run(["gh", "release", "upload", RELEASE_TAG, str(CACHE_PATH), "--clobber"], check=True)
