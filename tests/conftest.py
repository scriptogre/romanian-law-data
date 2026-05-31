"""Shared pytest fixtures."""

import json
from pathlib import Path

import pytest

FIXTURES_PATH = Path(__file__).parent / "fixtures.jsonl"


@pytest.fixture(scope="session")
def raw_acts() -> dict[str, dict]:
    """Hand-picked raw SOAP acts, keyed by `_id`. Covers every transform rule."""
    out: dict[str, dict] = {}
    with FIXTURES_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            act = json.loads(line)
            out[act["_id"]] = act
    return out
