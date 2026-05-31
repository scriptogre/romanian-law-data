"""
Deduplication.

SOAP returns the same act on adjacent pages sometimes (and CUANTUM TOTAL
acts share a boilerplate Titlu with different Emitent per political party).
The (Titlu, Emitent) tuple uniquely identifies a real act.

A second pass collapses code-vs-law twins: legislatie.just.ro lists each
Romanian "Cod" (Civil, Penal, Muncii, Fiscal, ...) twice — once as a LEGE
row, once as a CODUL X row with the same content. The CODUL row's Titlu
declares the relationship explicitly: e.g.
    "CODUL CIVIL din 17 iulie 2009 (*republicat*) ( LEGE nr. 287/2009 )"
We drop the CODUL row; the LEGE wrapper retains all metadata.
"""

import polars as pl

# Matches `( LEGE nr. N/YYYY )` (with optional dots in N) as it appears in
# republished code Titlus. The space inside the parens is required because
# legislatie.just.ro renders it consistently this way.
CODE_TWIN_REF_RE = r"\(\s*LEGE\s+nr\.\s*[\d.]+\s*/\s*\d{4}\s*\)"


def by_titlu_emitent(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Drop rows with null Titlu, then keep first occurrence per (Titlu, Emitent)."""
    return (
        lf.filter(pl.col("Titlu").is_not_null())
        .unique(subset=["Titlu", "Emitent"], keep="first", maintain_order=True)
    )


def collapse_code_twins(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Drop CODUL X rows that explicitly republish a LEGE (twin entries).

    A row is a twin iff TipAct starts with `COD` AND its Titlu carries a
    `( LEGE nr. N/YYYY )` back-reference. The LEGE row stays.
    """
    is_code = pl.col("TipAct").str.starts_with("COD")
    has_lege_ref = pl.col("Titlu").str.contains(CODE_TWIN_REF_RE)
    return lf.filter(~(is_code & has_lege_ref))
