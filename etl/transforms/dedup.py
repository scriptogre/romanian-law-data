"""
Deduplication.

SOAP returns the same act on adjacent pages sometimes (and CUANTUM TOTAL
acts share a boilerplate Titlu with different Emitent per political party).
The (Titlu, Emitent) tuple uniquely identifies a real act.
"""

import polars as pl


def by_titlu_emitent(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Drop rows with null Titlu, then keep first occurrence per (Titlu, Emitent)."""
    return (
        lf.filter(pl.col("Titlu").is_not_null())
        .unique(subset=["Titlu", "Emitent"], keep="first", maintain_order=True)
    )
