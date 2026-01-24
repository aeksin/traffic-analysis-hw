from __future__ import annotations

import logging

from ..context import PipelineContext
from .base import Handler

logger = logging.getLogger(__name__)


class CleanColumnsHandler(Handler):
    def _handle(self, ctx: PipelineContext) -> PipelineContext:
        if ctx.df is None:
            raise ValueError("DataFrame is not loaded")

        df = ctx.df.copy()

        df.columns = [str(c).strip() for c in df.columns]

        drop_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")
            logger.info("Dropped columns: %s", drop_cols)

        ctx.df = df
        return ctx
