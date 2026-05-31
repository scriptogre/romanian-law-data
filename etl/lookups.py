"""Small helper to load YAML lookup tables from data/lookups/."""

from functools import cache
from pathlib import Path

import yaml

LOOKUPS_DIR = Path(__file__).parent.parent / "data" / "lookups"


@cache
def load(name: str) -> dict:
    """Load `data/lookups/<name>.yaml`. Cached for the process lifetime."""
    with (LOOKUPS_DIR / f"{name}.yaml").open(encoding="utf-8") as f:
        return yaml.safe_load(f)
