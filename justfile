# Local development loop.
#
#   just test     — unit tests on hand-picked fixtures (<1 sec)
#   just local    — full pipeline on data/raw_documents/*.parquet (~5-10 min)
#   just extract  — sweep the portal into data/raw_documents/ (slow; CI shards it)
#
# `just local` needs data/raw_documents/*.parquet. Get them from the
# documents-cache release (raw_documents.parquet) or by running `just extract`.
# Edit-test loop: edit transform.py / load.py / lookup yaml, then `just test`,
# then `just local` to verify end-to-end before committing.

default:
    @just --list

# Unit tests on fixtures. Subsecond.
test:
    uv run pytest -q

# Sweep /Public/DetaliiDocument/{id} into data/raw_documents/*.parquet.
# One IP, rate-limited — a long job locally; CI shards it across runners.
extract:
    uv run python -m etl.extract documents

# Full local pipeline: cleanup + parse + parquet + Pandera + FTS, one process.
local:
    uv run python -m etl.transform
