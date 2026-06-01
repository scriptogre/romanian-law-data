"""
Stage 1 — extract.py

Thin dispatcher over the per-source extractions in `extracts/`:

    python -m etl.extract                 # soap  : SOAP corpus walk -> raw_acts.jsonl
    python -m etl.extract actiuni         # web   : fetch one /Public/actiuni* shard
    python -m etl.extract actiuni-merge   # web   : fold shards into cache + publish

Each source module owns its own client/session and resume state.
"""

import sys

from etl.extracts import soap, web

STAGES = {
    "soap": soap.run,
    "documents": soap.run,
    "actiuni": web.run,
    "web": web.run,
    "actiuni-merge": web.merge,
    "web-merge": web.merge,
}


def main() -> None:
    stage = sys.argv[1] if len(sys.argv) > 1 else "soap"
    runner = STAGES.get(stage)
    if runner is None:
        raise SystemExit(f"unknown stage {stage!r}; choose from {sorted(STAGES)}")
    runner()


if __name__ == "__main__":
    main()
