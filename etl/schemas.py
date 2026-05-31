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


class Acts(pa.DataFrameModel):
    id: int = pa.Field(unique=True, ge=1)
    type: str
    act_number: str = pa.Field(nullable=True)
    act_citation: str
    issuer: str
    title: str
    content: str
    adopted_at: date = pa.Field(nullable=True)
    published_at: date = pa.Field(nullable=True)
    effective_at: date = pa.Field(nullable=True)
    gazette_number: int = pa.Field(nullable=True, ge=1)
    status: str = pa.Field(nullable=True, isin=["în vigoare", "abrogat", "suspendat"])
    link: str
    synced_at: datetime


class Articles(pa.DataFrameModel):
    id: int = pa.Field(unique=True, ge=1)
    act_id: int = pa.Field(ge=1)
    article_number: int = pa.Field(nullable=True)
    article_variant: str = pa.Field(nullable=True)
    article_citation: str
    content: str = pa.Field(nullable=True)


class Paragraphs(pa.DataFrameModel):
    id: int = pa.Field(unique=True, ge=1)
    article_id: int = pa.Field(ge=1)
    paragraph_number: int = pa.Field(nullable=True)
    paragraph_citation: str
    content: str = pa.Field(nullable=True)


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

    Reads via Polars (eager — needed for Pandera Polars schema validation).
    Raises pandera.errors.SchemaError on column-level violations,
    ValueError on FK violations.
    """
    logger.info("schemas: validating parquets...")
    acts = pl.read_parquet(acts_path)
    articles = pl.read_parquet(articles_path)
    paragraphs = pl.read_parquet(paragraphs_path)

    Acts.validate(acts, lazy=True)
    Articles.validate(articles, lazy=True)
    Paragraphs.validate(paragraphs, lazy=True)
    check_referential_integrity(acts, articles, paragraphs)
    logger.success(
        f"schemas: OK — {acts.height} acte / {articles.height} articole / "
        f"{paragraphs.height} alineate"
    )
