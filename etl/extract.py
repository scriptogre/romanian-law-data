"""
Stage 1 — extract.py

Fetch every act from `legislatie.just.ro` via its SOAP API. Pages return 10
acts each. Output: `data/raw_acts.jsonl`, one JSON object per line, fields
mirror the SOAP response verbatim.
"""

import asyncio
import itertools
import json
import os
import random
import threading
import time
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

from loguru import logger
from requests import Session
from requests.adapters import HTTPAdapter
from zeep import Client
from zeep.helpers import serialize_object
from zeep.transports import Transport

WSDL = "https://legislatie.just.ro/apiws/FreeWebService.svc?wsdl"
PAGE_SIZE = 10
DEFAULT_CONCURRENCY = 16  # bump via ETL_CONCURRENCY env var when the runner allows more
POOL_SIZE = 128  # connection pool — must comfortably exceed DEFAULT_CONCURRENCY
DEFAULT_TOKEN_POOL_SIZE = 4  # rotate tokens to defeat per-token rate-limit (if any)
TOKEN_TTL_SECONDS = 60
MAX_RETRIES = 20
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0
BACKOFF_JITTER = 0.5  # multiplies delay by uniform(1 - JITTER, 1 + JITTER)

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "raw_acts.jsonl"
CURSOR_PATH = Path(__file__).parent.parent / "data" / ".extract_cursor"


@dataclass
class RawAct:
    """SOAP response shape. Field names match the API exactly."""

    Titlu: str
    Text: str
    TipAct: str
    Numar: str
    Emitent: str
    Publicatie: str
    DataVigoare: str
    LinkHtml: str


class LegislatieJustRoClient:
    def __init__(self, token_pool_size: int = DEFAULT_TOKEN_POOL_SIZE) -> None:
        self.session = Session()
        self.session.mount(
            "https://",
            HTTPAdapter(pool_connections=POOL_SIZE, pool_maxsize=POOL_SIZE),
        )
        # zeep's Transport.__init__ clobbers session User-Agent with "Zeep/...",
        # which legislatie.just.ro rejects with 403. Override AFTER Transport
        # is constructed, before any request fires.
        transport = Transport(session=self.session)
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        self.client = Client(WSDL, transport=transport)
        # Token pool: round-robin across N tokens to defeat per-token rate-limit
        # (if SOAP enforces one). 1 = legacy single-token behavior.
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
        # Fast path: pool full and no token expired.
        if (
            len(self.tokens) == self.token_pool_size
            and all(now - t <= TOKEN_TTL_SECONDS for t in self.token_times)
        ):
            return
        with self._token_lock:
            now = time.time()
            # Initialise on first call.
            while len(self.tokens) < self.token_pool_size:
                self.tokens.append(self.client.service.GetToken())
                self.token_times.append(now)
            # Refresh any expired entries within the same lock — keeps the
            # GetToken call rate bounded even under high fetch concurrency.
            for i in range(self.token_pool_size):
                if now - self.token_times[i] > TOKEN_TTL_SECONDS:
                    self.tokens[i] = self.client.service.GetToken()
                    self.token_times[i] = now

    def _next_token(self) -> str:
        self._ensure_tokens()
        idx = next(self._token_counter) % self.token_pool_size
        return self.tokens[idx]

    def fetch_page(self, page: int) -> list[dict]:
        """Fetch one page of acts. Exponential backoff with jitter on failure."""
        SearchModel = self.client.get_type(
            "{http://schemas.datacontract.org/2004/07/FreeWebService}CompositeType"
        )
        model = SearchModel(RezultatePagina=PAGE_SIZE, NumarPagina=page)

        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.service.Search(
                    SearchModel=model,
                    tokenKey=self._next_token(),
                )
                return [serialize_object(item) for item in (result or [])]
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    raise
                base = min(MAX_BACKOFF, INITIAL_BACKOFF * (2**attempt))
                delay = base * random.uniform(1 - BACKOFF_JITTER, 1 + BACKOFF_JITTER)
                logger.warning(
                    f"page {page} retry {attempt + 1}/{MAX_RETRIES} in {delay:.1f}s"
                )
                time.sleep(delay)
        return []


def _read_cursor() -> int | None:
    if not CURSOR_PATH.exists():
        return None
    try:
        return int(CURSOR_PATH.read_text().strip())
    except (OSError, ValueError):
        return None


def _write_cursor(page: int) -> None:
    CURSOR_PATH.write_text(str(page))


async def extract_all(
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    token_pool_size: int = DEFAULT_TOKEN_POOL_SIZE,
    start_page: int | None = None,
    max_acts: int | None = None,
    output_path: Path = OUTPUT_PATH,
) -> int:
    """Walk every page, append each act as JSONL. Returns total acts written.

    Resumable. If `data/.extract_cursor` exists, picks up from the page AFTER
    the last successfully-completed batch and appends to the existing JSONL.
    Pass `start_page` explicitly to override / force a fresh run.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    client = LegislatieJustRoClient(token_pool_size=token_pool_size)
    total = 0

    cursor = _read_cursor()
    resuming = cursor is not None and start_page is None
    if start_page is None:
        page_cursor = (cursor + 1) if cursor is not None else 1
    else:
        page_cursor = start_page

    mode = "a" if resuming else "w"
    state = (
        f"resuming from page {page_cursor}, cursor was {cursor}"
        if resuming
        else f"fresh run from page {page_cursor}"
    )
    logger.info(
        f"extract: start ({state}, concurrency={concurrency}, tokens={token_pool_size})"
    )

    start_time = time.time()
    with output_path.open(mode, encoding="utf-8") as fp:
        while True:
            batch = range(page_cursor, page_cursor + concurrency)
            tasks = [asyncio.to_thread(client.fetch_page, p) for p in batch]
            results = await asyncio.gather(*tasks)
            acts = list(chain.from_iterable(results))

            if not acts:
                logger.info(f"empty batch at pages {list(batch)}, stopping")
                CURSOR_PATH.unlink(missing_ok=True)
                break

            for act in acts:
                fp.write(json.dumps(act, ensure_ascii=False, default=str) + "\n")
            fp.flush()
            total += len(acts)
            _write_cursor(page_cursor + concurrency - 1)

            elapsed = time.time() - start_time
            rate = total / elapsed if elapsed > 0 else 0.0
            logger.info(
                f"pages [{batch.start}..{batch.stop - 1}]: +{len(acts)} acts  "
                f"total={total:>6d}  rate={rate:>5.0f} acts/s"
            )

            if max_acts is not None and total >= max_acts:
                logger.info(f"reached max_acts={max_acts}, stopping")
                break

            page_cursor += concurrency

    return total


def main() -> None:
    concurrency = int(os.environ.get("ETL_CONCURRENCY", DEFAULT_CONCURRENCY))
    token_pool_size = int(os.environ.get("ETL_TOKEN_POOL_SIZE", DEFAULT_TOKEN_POOL_SIZE))
    # `os.environ.get` (not `in os.environ`) treats unset and empty-string the
    # same way — the workflow sets ETL_MAX_ACTS to "" on scheduled runs.
    max_acts_raw = os.environ.get("ETL_MAX_ACTS")
    max_acts = int(max_acts_raw) if max_acts_raw else None

    start_page_raw = os.environ.get("ETL_START_PAGE")
    start_page = int(start_page_raw) if start_page_raw else None

    total = asyncio.run(
        extract_all(
            concurrency=concurrency,
            token_pool_size=token_pool_size,
            start_page=start_page,
            max_acts=max_acts,
        )
    )
    logger.success(f"extract: DONE — {total} acts → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
