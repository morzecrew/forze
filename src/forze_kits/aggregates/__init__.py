"""Aggregate operation kits: registries, facades, and kernel operation ids."""

from .kit import AggregateKit
from .repository import AggregateRepository, aggregate_repository

# ----------------------- #

__all__ = [
    "AggregateKit",
    "AggregateRepository",
    "aggregate_repository",
]
