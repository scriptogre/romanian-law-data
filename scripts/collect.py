"""
Stage 1 — collect.py

Fetch every act from `legislatie.just.ro` via its SOAP API. Pages return 10
acts each. Output: `data/raw_acts.jsonl`, one JSON object per line, fields
mirror the SOAP response verbatim.

Ported from the previous Django version's `LegislatieJustRoClient`.
"""

import asyncio
import json
import os
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
DEFAULT_CONCURRENCY = 8
TOKEN_TTL_SECONDS = 60
MAX_RETRIES = 20
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "raw_acts.jsonl"
CURSOR_PATH = Path(__file__).parent.parent / "data" / ".collect_cursor"


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
    def __init__(self) -> None:
        self.session = Session()
        self.session.mount(
            "https://",
            HTTPAdapter(pool_connections=50, pool_maxsize=50),
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
        self.token: str | None = None
        self.token_time = 0.0

    def __del__(self) -> None:
        self.session.close()

    def _refresh_token(self) -> None:
        if time.time() - self.token_time > TOKEN_TTL_SECONDS:
            self.token = self.client.service.GetToken()
            self.token_time = time.time()

    def fetch_page(self, page: int) -> list[dict]:
        """Fetch one page of acts. Exponential backoff on failure."""
        SearchModel = self.client.get_type(
            "{http://schemas.datacontract.org/2004/07/FreeWebService}CompositeType"
        )
        model = SearchModel(RezultatePagina=PAGE_SIZE, NumarPagina=page)

        for attempt in range(MAX_RETRIES):
            try:
                self._refresh_token()
                result = self.client.service.Search(
                    SearchModel=model,
                    tokenKey=self.token,
                )
                return [serialize_object(item) for item in (result or [])]
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    raise
                delay = min(MAX_BACKOFF, INITIAL_BACKOFF * (2**attempt))
                logger.warning(f"page {page} retry {attempt + 1}/{MAX_RETRIES} in {delay}s")
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


async def collect_all(
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    start_page: int | None = None,
    max_acts: int | None = None,
    output_path: Path = OUTPUT_PATH,
) -> int:
    """Walk every page, append each act as JSONL. Returns total acts written.

    Resumable. If `data/.collect_cursor` exists, picks up from the page AFTER
    the last successfully-completed batch and appends to the existing JSONL.
    Pass `start_page` explicitly to override / force a fresh run.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    client = LegislatieJustRoClient()
    total = 0

    cursor = _read_cursor()
    resuming = cursor is not None and start_page is None
    if start_page is None:
        page_cursor = (cursor + 1) if cursor is not None else 1
    else:
        page_cursor = start_page

    mode = "a" if resuming else "w"
    if resuming:
        logger.info(f"resuming from page {page_cursor} (cursor was {cursor})")
    else:
        logger.info(f"fresh run from page {page_cursor}")

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

            logger.info(f"pages {list(batch)}: +{len(acts)} acts (total {total})")

            if max_acts is not None and total >= max_acts:
                logger.info(f"reached max_acts={max_acts}, stopping")
                break

            page_cursor += concurrency

    return total


def main() -> None:
    concurrency = int(os.environ.get("ETL_CONCURRENCY", DEFAULT_CONCURRENCY))
    max_acts = int(os.environ["ETL_MAX_ACTS"]) if "ETL_MAX_ACTS" in os.environ else None
    start_page = int(os.environ["ETL_START_PAGE"]) if "ETL_START_PAGE" in os.environ else None

    total = asyncio.run(
        collect_all(
            concurrency=concurrency,
            start_page=start_page,
            max_acts=max_acts,
        )
    )
    logger.success(f"collected {total} acts → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
