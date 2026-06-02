"""
Stage 1 — extracts/documents.py

Fetch every act from `legislatie.just.ro` via its SOAP API (FreeWebService).
Output: parquet parts under `data/raw_documents/`, the raw SOAP fields verbatim.
Stage 2 (transform) reads them as a glob.

SHARDED across runners (= IPs): pages are dense and 1-indexed, so shard `s` of
`N` walks pages s+1, s+1+N, s+1+2N, ... until it hits an empty page (= past the
end). Each runner writes its own part files (named by first page, so no
collisions); the publish job reads them all. The SOAP endpoint scales per IP
(rotating tokens on one IP does not), so N runners ~= Nx throughput.
"""

import asyncio
import itertools
import os
import random
import threading
import time
from itertools import chain
from pathlib import Path

import polars as pl
from loguru import logger
from requests import Session
from requests.adapters import HTTPAdapter
from zeep import Client
from zeep.helpers import serialize_object
from zeep.transports import Transport

WSDL = "https://legislatie.just.ro/apiws/FreeWebService.svc?wsdl"
PAGE_SIZE = 10
POOL_SIZE = 128  # connection pool, comfortably above concurrency
TOKEN_TTL_SECONDS = 60
MAX_RETRIES = 20
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0
BACKOFF_JITTER = 0.5  # multiplies delay by uniform(1 - JITTER, 1 + JITTER)

# SOAP record fields, stored verbatim. Transform reads these column names.
SOAP_FIELDS = ("Titlu", "Text", "TipAct", "Numar", "Emitent", "Publicatie", "DataVigoare", "LinkHtml")

REPO_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw_documents"


class LegislatieJustRoClient:
    def __init__(self, token_pool_size: int) -> None:
        self.session = Session()
        self.session.mount("https://", HTTPAdapter(pool_connections=POOL_SIZE, pool_maxsize=POOL_SIZE))
        # zeep's Transport clobbers the User-Agent with "Zeep/...", which the site
        # rejects with 403. Override AFTER Transport is built, before any request.
        transport = Transport(session=self.session)
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.client = Client(WSDL, transport=transport)
        self.token_pool_size = max(1, token_pool_size)
        self.tokens: list[str] = []
        self.token_times: list[float] = []
        self._token_counter = itertools.count()
        self._token_lock = threading.Lock()

    def __del__(self) -> None:
        self.session.close()

    def _ensure_tokens(self) -> None:
        """Initialise / refresh the token pool. Thread-safe."""
        now = time.time()
        if len(self.tokens) == self.token_pool_size and all(
            now - t <= TOKEN_TTL_SECONDS for t in self.token_times
        ):
            return
        with self._token_lock:
            now = time.time()
            while len(self.tokens) < self.token_pool_size:
                self.tokens.append(self.client.service.GetToken())
                self.token_times.append(now)
            for i in range(self.token_pool_size):
                if now - self.token_times[i] > TOKEN_TTL_SECONDS:
                    self.tokens[i] = self.client.service.GetToken()
                    self.token_times[i] = now

    def _next_token(self) -> str:
        self._ensure_tokens()
        return self.tokens[next(self._token_counter) % self.token_pool_size]

    def fetch_page(self, page: int) -> list[dict]:
        """Fetch one page of acts. Exponential backoff with jitter on failure."""
        SearchModel = self.client.get_type(
            "{http://schemas.datacontract.org/2004/07/FreeWebService}CompositeType"
        )
        model = SearchModel(RezultatePagina=PAGE_SIZE, NumarPagina=page)

        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.service.Search(SearchModel=model, tokenKey=self._next_token())
                return [serialize_object(item) for item in (result or [])]
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    raise
                base = min(MAX_BACKOFF, INITIAL_BACKOFF * (2**attempt))
                delay = base * random.uniform(1 - BACKOFF_JITTER, 1 + BACKOFF_JITTER)
                logger.warning(f"page {page} retry {attempt + 1}/{MAX_RETRIES} in {delay:.1f}s")
                time.sleep(delay)
        return []


def _to_rows(acts: list[dict]) -> list[dict]:
    """Normalise SOAP dicts to the fixed string schema (stable parquet columns)."""
    return [{f: (None if a.get(f) is None else str(a.get(f))) for f in SOAP_FIELDS} for a in acts]


async def _extract_shard(
    client: LegislatieJustRoClient, shard: int, shards: int, concurrency: int, limit: int | None
) -> int:
    """Walk this shard's strided pages, writing parquet parts. Returns acts written."""
    schema = {f: pl.Utf8 for f in SOAP_FIELDS}
    page = shard + 1  # pages are 1-indexed
    total = 0
    start_time = time.time()

    while True:
        batch_pages = [page + i * shards for i in range(concurrency)]
        results = await asyncio.gather(*(asyncio.to_thread(client.fetch_page, p) for p in batch_pages))
        acts = list(chain.from_iterable(results))
        if not acts:
            break

        part = RAW_DIR / f"part_{page:06d}.parquet"
        pl.DataFrame(_to_rows(acts), schema=schema).write_parquet(part, compression="zstd")
        total += len(acts)
        rate = total / (time.time() - start_time) if total else 0
        logger.info(f"shard {shard}/{shards}: +{len(acts)} (total {total}, {rate:.0f}/s) -> {part.name}")

        if limit is not None and total >= limit:
            break
        if any(not r for r in results):  # an empty page in the batch = past the end
            break
        page += concurrency * shards

    logger.success(f"shard {shard}/{shards} done: {total} acts")
    return total


def run() -> None:
    concurrency = int(os.environ.get("ETL_DOCUMENTS_CONCURRENCY", 16))
    tokens = int(os.environ.get("ETL_DOCUMENTS_TOKENS", 4))
    shard = int(os.environ.get("ETL_DOCUMENTS_SHARD", 0))
    shards = int(os.environ.get("ETL_DOCUMENTS_SHARDS", 1))
    limit_raw = os.environ.get("ETL_DOCUMENTS_LIMIT")
    limit = int(limit_raw) if limit_raw else None

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"documents: shard {shard}/{shards}, concurrency {concurrency}, tokens {tokens}")
    client = LegislatieJustRoClient(token_pool_size=tokens)
    asyncio.run(_extract_shard(client, shard, shards, concurrency, limit))
