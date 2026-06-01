"""
Probe: is legislatie.just.ro's `actiuni` rate-limit per-IP or global, and WHY do
slow runners fail (429 rate-limit vs 403 block vs timeout vs network)?

extract.py assumes a single global per-second cap. The probe-actiuni workflow
runs this identical workload from N runners (N IPs) at once. Read per-runner:
    rate_req_s  : flat as N grows -> per-IP cap; drops ~1/N -> global cap
    outcomes    : the raw HTTP status / exception mix (the "why")

Single attempt per request, no retry/backoff, so the rate and the status mix
reflect the server's actual response rather than client backoff.

Usage: uv run python -m etl.probe_actiuni --requests 300 --concurrency 20
"""

import argparse
import asyncio
import os
import time
from collections import Counter
from itertools import cycle

from loguru import logger

from etl.extract import ACTIONS_BASE, LegislatieJustRoClient, _act_id_from_link


async def _gather_ids(client: LegislatieJustRoClient, want: int) -> list[str]:
    """Pull real act ids off SOAP pages until we have `want` (cap 60 pages)."""
    ids: list[str] = []
    page = 1
    while len(ids) < want and page <= 60:
        acts = await asyncio.to_thread(client.fetch_page, page)
        if not acts:
            break
        ids += [i for a in acts if (i := _act_id_from_link(a.get("LinkHtml")))]
        page += 1
    return ids


def _post_once(client: LegislatieJustRoClient, endpoint: str, act_id: str) -> str:
    """One raw POST. Returns the HTTP status as a string, or the exception name."""
    url = f"{ACTIONS_BASE}/{endpoint}"
    try:
        response = client.session.post(url, data={"contor": act_id}, timeout=30)
        return str(response.status_code)
    except Exception as exc:
        return type(exc).__name__


async def probe(total_requests: int, concurrency: int, endpoint: str) -> None:
    client = LegislatieJustRoClient()
    ids = await _gather_ids(client, total_requests)
    if not ids:
        raise SystemExit("probe: gathered no act ids")

    pool = cycle(ids)
    targets = [next(pool) for _ in range(total_requests)]

    sem = asyncio.Semaphore(concurrency)
    outcomes: Counter[str] = Counter()

    async def one(act_id: str) -> None:
        async with sem:
            outcome = await asyncio.to_thread(_post_once, client, endpoint, act_id)
            outcomes[outcome] += 1

    start = time.time()
    await asyncio.gather(*(one(a) for a in targets))
    elapsed = time.time() - start
    rate = total_requests / elapsed if elapsed else 0.0

    label = os.environ.get("PROBE_LABEL", "solo")
    breakdown = " ".join(f"{code}={n}" for code, n in sorted(outcomes.items()))
    logger.success(
        f"[{label}] {total_requests} req @ concurrency {concurrency} on {endpoint}: "
        f"{elapsed:.1f}s -> {rate:.1f} req/s | {breakdown}"
    )
    print(f"PROBE_RESULT label={label} rate_req_s={rate:.2f} elapsed_s={elapsed:.1f} outcomes[{breakdown}]")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=300)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--endpoint", default="actiuniSuferite", choices=("actiuniSuferite", "actiuniInduse"))
    args = ap.parse_args()
    asyncio.run(probe(args.requests, args.concurrency, args.endpoint))


if __name__ == "__main__":
    main()
