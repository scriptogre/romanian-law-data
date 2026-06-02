"""
Stage 1 — extract.py

Thin dispatcher over the per-domain extractions in `extracts/`:

    python -m etl.extract documents             # SOAP corpus walk -> data/raw_documents/
    python -m etl.extract relationships         # fetch one /Public/actiuni* shard
    python -m etl.extract relationships-merge   # fold shards into the cache + publish

Each domain module owns its own client/session and resume state.
"""

import sys

from etl.extracts import documents, relationships

STAGES = {
    "documents": documents.run,
    "relationships": relationships.run,
    "relationships-merge": relationships.merge,
}


def main() -> None:
    stage = sys.argv[1] if len(sys.argv) > 1 else "documents"
    runner = STAGES.get(stage)
    if runner is None:
        raise SystemExit(f"unknown stage {stage!r}; choose from {sorted(STAGES)}")
    runner()


if __name__ == "__main__":
    main()
