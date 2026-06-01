"""
Probe: is legislatie.just.ro's `actiuni` rate-limit per-IP or global?

extract.py assumes a single global per-second cap (so concurrency on one IP
saturates it). That assumption decides everything: if the cap is per-IP, a
matrix of N runners (N IPs) fetches ~Nx faster; if it is global, parallel
runners do not help and may get blocked.

Test it: the probe-actiuni workflow runs this identical workload from N runners
at once. Read the per-runner req/s across runs:
    per-IP cap  -> per-runner rate stays flat as N grows (total ~Nx)
    global cap  -> per-runner rate drops ~1/N (total flat)

Throttling shows up as a LOWER rate here (fetch_action backs off internally
rather than erroring), which is exactly what we want to measure.

Usage: uv run python -m etl.probe_actiuni --requests 300 --concurrency 20
"""

import argparse
import asyncio
import os
import time
from itertools import cycle

from loguru import logger

from etl.extract import LegislatieJustRoClient, _act_id_from_link


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


async def probe(total_requests: int, concurrency: int, endpoint: str) -> None:
    client = LegislatieJustRoClient()
    ids = await _gather_ids(client, total_requests)
    if not ids:
        raise SystemExit("probe: gathered no act ids")

    pool = cycle(ids)
    targets = [next(pool) for _ in range(total_requests)]

    sem = asyncio.Semaphore(concurrency)
    ok = empty = 0

    async def one(act_id: str) -> None:
        nonlocal ok, empty
        async with sem:
            html = await asyncio.to_thread(client.fetch_action, endpoint, act_id)
            if html:
                ok += 1
            else:
                empty += 1  # genuinely-empty action list OR gave-up after retries

    start = time.time()
    await asyncio.gather(*(one(a) for a in targets))
    elapsed = time.time() - start
    rate = total_requests / elapsed if elapsed else 0.0

    label = os.environ.get("PROBE_LABEL", "solo")
    logger.success(
        f"[{label}] {total_requests} req @ concurrency {concurrency} on {endpoint}: "
        f"{elapsed:.1f}s -> {rate:.1f} req/s (ok={ok} empty/giveup={empty})"
    )
    print(f"PROBE_RESULT label={label} rate_req_s={rate:.2f} elapsed_s={elapsed:.1f} ok={ok} empty={empty}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=300)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--endpoint", default="actiuniSuferite", choices=("actiuniSuferite", "actiuniInduse"))
    args = ap.parse_args()
    asyncio.run(probe(args.requests, args.concurrency, args.endpoint))


if __name__ == "__main__":
    main()
