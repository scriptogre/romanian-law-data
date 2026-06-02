"""
Pandera schemas for the three final parquets.

Run via `validate_all(...)` after `load.write_parquets()`. Any contract
violation raises and fails the workflow loudly. Foreign-key integrity is
checked separately by `check_referential_integrity()` since Pandera doesn't
have built-in FK constraints.

Schema constraints are conservative for now — Phase 5 will tighten them
(e.g. clamp `effective_at` upper bound once we kill the year-6201 outlier).
"""

from datetime import date, datetime
from pathlib import Path

import pandera.polars as pa
import polars as pl
from loguru import logger


# `content` is intentionally omitted from every schema. The PyArrow writer
# already enforces `string` dtype on write, so the only thing Pandera would add
# is "is a string" — at the cost of materializing 3-5 GB of body text into
# memory during validation, which OOMs the 16 GB CI runner.


class Acts(pa.DataFrameModel):
    id: int = pa.Field(unique=True, ge=1)
    type: str
    act_number: str = pa.Field(nullable=True)
    act_citation: str
    issuer: str
    title: str
    adopted_at: date = pa.Field(nullable=True)
    published_at: date = pa.Field(nullable=True)
    effective_at: date = pa.Field(nullable=True)
    gazette_number: int = pa.Field(nullable=True, ge=1)
    status: str = pa.Field(nullable=True, isin=["in_force", "repealed", "suspended"])
    link: str
    synced_at: datetime


class Articles(pa.DataFrameModel):
    id: int = pa.Field(unique=True, ge=1)
    act_id: int = pa.Field(ge=1)
    article_number: int = pa.Field(nullable=True)
    article_variant: str = pa.Field(nullable=True)
    article_citation: str


class Paragraphs(pa.DataFrameModel):
    id: int = pa.Field(unique=True, ge=1)
    article_id: int = pa.Field(ge=1)
    paragraph_number: int = pa.Field(nullable=True)
    paragraph_citation: str


def check_referential_integrity(
    acts: pl.DataFrame, articles: pl.DataFrame, paragraphs: pl.DataFrame
) -> None:
    """All FK references must resolve. Raises `ValueError` listing the orphans."""
    act_ids = set(acts["id"].to_list())
    orphan_articles = articles.filter(~pl.col("act_id").is_in(list(act_ids)))
    if orphan_articles.height:
        raise ValueError(
            f"{orphan_articles.height} articole rows reference non-existent acte.id; "
            f"first offenders: {orphan_articles['act_id'].head(5).to_list()}"
        )
    article_ids = set(articles["id"].to_list())
    orphan_paragraphs = paragraphs.filter(~pl.col("article_id").is_in(list(article_ids)))
    if orphan_paragraphs.height:
        raise ValueError(
            f"{orphan_paragraphs.height} alineate rows reference non-existent articole.id; "
            f"first offenders: {orphan_paragraphs['article_id'].head(5).to_list()}"
        )


def validate_parquets(
    acts_path: Path, articles_path: Path, paragraphs_path: Path
) -> None:
    """Validate the three written parquets against the schemas + FK integrity.

    Pandera Polars needs an eager DataFrame to validate, but the `content`
    columns dominate memory (3-5 GB uncompressed) and have no schema check
    beyond "is a string" — which the parquet writer already enforced. We
    project them out before collect so the 16 GB CI runner survives.
    Raises pandera.errors.SchemaError on column-level violations,
    ValueError on FK violations.
    """
    logger.info("schemas: validating parquets...")
    acts = pl.scan_parquet(acts_path).drop("content").collect()
    articles = pl.scan_parquet(articles_path).drop("content").collect()
    paragraphs = pl.scan_parquet(paragraphs_path).drop("content").collect()

    Acts.validate(acts, lazy=True)
    Articles.validate(articles, lazy=True)
    Paragraphs.validate(paragraphs, lazy=True)
    check_referential_integrity(acts, articles, paragraphs)
    logger.success(
        f"schemas: OK — {acts.height} acte / {articles.height} articole / "
        f"{paragraphs.height} alineate"
    )
