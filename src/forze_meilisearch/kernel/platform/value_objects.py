"""Meilisearch client configuration."""

from datetime import timedelta
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MeilisearchConfig:
    """Connection settings for :class:`~forze_meilisearch.kernel.platform.client.MeilisearchClient`."""

    timeout_seconds: timedelta = attrs.field(default=timedelta(seconds=30))
    """HTTP client timeout."""
