"""
Probe: find the actiuni concurrency sweet spot, and see WHY requests fail.

The server returns 503 under concurrent load (not 403/429/timeout): too much
concurrency = more 503s, too little = idle capacity. The sweet spot maximises
GOODPUT (successful 200s per second), not raw request rate.

`--sweep "2,5,10,20,40"` runs the same workload at each concurrency level
back-to-back in ONE process (same IP, same minute), so concurrency is the only
variable. Single attempt per request, no retry, so the 503 rate is the raw
server signal rather than client backoff.

Usage:
  uv run python -m etl.probe_actiuni --sweep "2,5,10,20,40" --requests 300
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


async def _run_level(
    client: LegislatieJustRoClient, targets: list[str], concurrency: int, endpoint: str
) -> None:
    sem = asyncio.Semaphore(concurrency)
    outcomes: Counter[str] = Counter()

    async def one(act_id: str) -> None:
        async with sem:
            outcomes[await asyncio.to_thread(_post_once, client, endpoint, act_id)] += 1

    start = time.time()
    await asyncio.gather(*(one(a) for a in targets))
    elapsed = time.time() - start

    rate = len(targets) / elapsed if elapsed else 0.0
    goodput = outcomes.get("200", 0) / elapsed if elapsed else 0.0
    fail_pct = 100 * (1 - outcomes.get("200", 0) / len(targets)) if targets else 0.0
    breakdown = " ".join(f"{code}={n}" for code, n in sorted(outcomes.items()))
    label = os.environ.get("PROBE_LABEL", "solo")

    logger.success(
        f"[{label}] concurrency={concurrency}: {rate:.0f} req/s, "
        f"goodput={goodput:.0f} ok/s, fail={fail_pct:.0f}% | {breakdown}"
    )
    print(
        f"PROBE_RESULT label={label} concurrency={concurrency} "
        f"rate_req_s={rate:.1f} goodput_ok_s={goodput:.1f} fail_pct={fail_pct:.0f} outcomes[{breakdown}]"
    )


async def probe(levels: list[int], total_requests: int, endpoint: str) -> None:
    client = LegislatieJustRoClient()
    ids = await _gather_ids(client, total_requests)
    if not ids:
        raise SystemExit("probe: gathered no act ids")

    pool = cycle(ids)
    targets = [next(pool) for _ in range(total_requests)]

    for concurrency in levels:
        await _run_level(client, targets, concurrency, endpoint)
        await asyncio.sleep(2)  # let the server breathe between levels


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", default="20", help="comma-separated concurrency levels")
    ap.add_argument("--requests", type=int, default=300)
    ap.add_argument("--endpoint", default="actiuniSuferite", choices=("actiuniSuferite", "actiuniInduse"))
    args = ap.parse_args()
    levels = [int(x) for x in args.sweep.split(",") if x.strip()]
    asyncio.run(probe(levels, args.requests, args.endpoint))


if __name__ == "__main__":
    main()
