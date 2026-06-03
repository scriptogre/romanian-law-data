"""
Stage 1 (alt) — extracts/relationships.py

Fetch each act's OUTGOING relationships from legislatie.just.ro's own web
endpoints, separate from the SOAP API in documents.py. For each act:

    affects (actiuniInduse) = changes THIS act makes to others: "repeals ...", ...
    cites   (referaPe)      = acts THIS act references

We fetch only the outgoing side. The incoming side (who changes / cites this act)
is the exact inverse and is derived in the transform layer, not fetched — verified
symmetric (changes 100% across the whole corpus, citations 45/45 sampled). Output:
a durable cache parquet (document_id -> affects_html, cites_html, fetched_at)
published as a release asset.

THROTTLE: the site runs a per-IP token bucket - a few hundred acts fetch fast,
then that IP is choked to ~1 req/s. One IP cannot do the backfill. So the work
is SHARDED across many runners (= many IPs): each fetches its slice
(ids[shard::shards]) into a shard parquet; a separate merge step folds the shards
into the cache and publishes. Resumable: `run()` fetches only acts NOT already
cached, so re-dispatching reshards whatever is still missing.

Within a runner: SHORT connect timeout (a refused connection fails in 5s, not
30s), modest concurrency, a few retry passes. Progress (ok/processed, rate, ETA)
logs every few seconds, and the shard parquet is checkpointed to disk per chunk.

Env:
  ETL_RELATIONSHIPS_MODE         backfill | delta (default delta)
  ETL_RELATIONSHIPS_SHARD        this runner's index, 0-based (default 0)
  ETL_RELATIONSHIPS_SHARDS       total runners (default 1)
  ETL_RELATIONSHIPS_LIMIT        cap ids (smoke test)
  ETL_RELATIONSHIPS_RELEASE_TAG  merge stage publishes the cache to this tag (CI only)
"""

import asyncio
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from requests.adapters import HTTPAdapter

ENDPOINTS = ("actiuniInduse", "referaPe")  # outgoing: changes-made, citations-made
BASE = "https://legislatie.just.ro/Public"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

CONCURRENCY = int(os.environ.get("ETL_RELATIONSHIPS_CONCURRENCY", 8))
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30
RETRY_PASSES = 3
CHUNK = int(os.environ.get("ETL_RELATIONSHIPS_CHUNK", 5000))

REPO_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = REPO_ROOT / "data"
CORPUS_PATH = DATA_DIR / "documents.parquet"  # the document list the transform emits
CACHE_PATH = DATA_DIR / "raw_relationships.parquet"
RECONCILE_FRACTION = float(os.environ.get("ETL_RELATIONSHIPS_RECONCILE", 0.0333))  # ~1/30 -> ~monthly
RELEASE_TAG = os.environ.get("ETL_RELATIONSHIPS_RELEASE_TAG")

CACHE_SCHEMA = {
    "document_id": pl.Utf8,
    "affects_html": pl.Utf8,
    "cites_html": pl.Utf8,
    "fetched_at": pl.Datetime,
}


# --- cache -----------------------------------------------------------------

def load_cache() -> pl.DataFrame:
    """Load the cache, but only if it matches the current schema.

    A schema change (e.g. swapping the incoming endpoint for citations) makes the
    old cache incompatible; treat it as empty so the backfill rebuilds from scratch.
    """
    if not CACHE_PATH.exists():
        return pl.DataFrame(schema=CACHE_SCHEMA)
    cached = pl.read_parquet(CACHE_PATH)
    if set(cached.columns) != set(CACHE_SCHEMA):
        return pl.DataFrame(schema=CACHE_SCHEMA)
    return cached.select(list(CACHE_SCHEMA))


def merge_cache(base: pl.DataFrame, new_rows: list[dict]) -> pl.DataFrame:
    """Fold freshly-fetched rows in. A re-fetched document_id overrides the old
    one; dup ids within the incoming batch collapse to the last (dup-safe)."""
    if not new_rows:
        return base
    incoming = pl.DataFrame(new_rows, schema=CACHE_SCHEMA).unique(subset=["document_id"], keep="last")
    keep = base.filter(~pl.col("document_id").is_in(incoming["document_id"]))
    return pl.concat([keep, incoming])


def ids_to_fetch(all_ids: list[str], cache: pl.DataFrame, reconcile: float) -> list[str]:
    """New (uncached) acts, plus a reconcile slice (oldest-fetched first)."""
    have = set(cache["document_id"].to_list())
    new = [i for i in all_ids if i not in have]

    stale: list[str] = []
    if reconcile > 0 and len(cache):
        n = max(1, int(len(cache) * reconcile))
        stale = cache.sort("fetched_at").head(n)["document_id"].to_list()

    return list(dict.fromkeys(new + stale))


# --- fetch -----------------------------------------------------------------

def _session() -> requests.Session:
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=64, pool_maxsize=64))
    session.headers["User-Agent"] = UA
    return session


def _fetch(session: requests.Session, endpoint: str, document_id: str) -> str | None:
    """POST one endpoint. HTML on 200, None on any failure (retried next pass)."""
    try:
        response = session.post(
            f"{BASE}/{endpoint}", data={"contor": document_id}, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
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

    async def one(document_id: str) -> None:
        nonlocal last_log
        async with sem:
            affects = await asyncio.to_thread(_fetch, session, "actiuniInduse", document_id)
            cites = await asyncio.to_thread(_fetch, session, "referaPe", document_id)
            result[document_id] = (affects, cites)

            now = time.time()
            if now - last_log >= 15:
                last_log = now
                ok = sum(1 for a, b in result.values() if a is not None and b is not None)
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
        for document_id, (affects, cites) in (await _one_pass(session, pending)).items():
            if affects is not None and cites is not None:
                done[document_id] = (affects, cites)
        pending = [a for a in pending if a not in done]
        logger.info(f"pass {attempt}: {len(done)}/{len(ids)} ok, {len(pending)} left")
        if not pending:
            break

    now = datetime.now(UTC)
    return [
        {"document_id": d, "affects_html": a, "cites_html": b, "fetched_at": now}
        for d, (a, b) in done.items()
    ]


# --- orchestration ---------------------------------------------------------

def _shard_path(shard: int) -> Path:
    return DATA_DIR / f"relationships_shard_{shard}.parquet"


def run() -> None:
    """Fetch this shard's uncached acts, in chunks, into its shard parquet.

    Does NOT publish or touch the cache; the merge stage folds shards in. Sharded
    so many runners (= many IPs) split the work past the per-IP throttle.
    """
    mode = os.environ.get("ETL_RELATIONSHIPS_MODE", "delta")
    shard = int(os.environ.get("ETL_RELATIONSHIPS_SHARD", 0))
    shards = int(os.environ.get("ETL_RELATIONSHIPS_SHARDS", 1))
    limit_raw = os.environ.get("ETL_RELATIONSHIPS_LIMIT")

    corpus = pl.read_parquet(CORPUS_PATH)
    all_ids = corpus["link"].str.extract(r"(\d+)\s*$", 1).drop_nulls().to_list()
    cache = load_cache()

    reconcile = RECONCILE_FRACTION if mode == "delta" else 0.0
    ids = ids_to_fetch(all_ids, cache, reconcile)[shard::shards]
    if limit_raw:
        ids = ids[: int(limit_raw)]
    logger.info(f"relationships {mode} shard {shard}/{shards}: {len(ids)} to fetch")

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
    shard_files = sorted(DATA_DIR.glob("relationships_shard_*.parquet"))
    rows = (
        pl.concat([pl.read_parquet(f) for f in shard_files]).to_dicts() if shard_files else []
    )
    merged = merge_cache(cache, rows)
    merged.write_parquet(CACHE_PATH, compression="zstd")
    logger.success(f"merge: cache {len(cache)} -> {len(merged)} (+{len(rows)} from {len(shard_files)} shards)")

    corpus = pl.read_parquet(CORPUS_PATH)
    all_ids = corpus["link"].str.extract(r"(\d+)\s*$", 1).drop_nulls().to_list()
    remaining = len(set(all_ids) - set(merged["document_id"].to_list()))
    logger.info(f"REMAINING={remaining}")

    if RELEASE_TAG:
        exists = subprocess.run(["gh", "release", "view", RELEASE_TAG], capture_output=True).returncode == 0
        if not exists:
            subprocess.run(
                ["gh", "release", "create", RELEASE_TAG, "--title", "relationships cache",
                 "--notes", "Per-act relationship HTML from legislatie.just.ro /Public. Rebuilt by sync-relationships."],
                check=True,
            )
        subprocess.run(["gh", "release", "upload", RELEASE_TAG, str(CACHE_PATH), "--clobber"], check=True)
