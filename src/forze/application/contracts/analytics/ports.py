from typing import Protocol

from pydantic import BaseModel

# ----------------------- #


class AnalyticsQueryPort[R: BaseModel](Protocol):
    """Port for querying analytics data."""


# ....................... #


class AnalyticsIngestPort[I: BaseModel](Protocol):
    """Port for ingesting analytics data."""
