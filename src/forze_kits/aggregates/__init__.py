"""Aggregate operation kits: registries, facades, and kernel operation ids."""

from .kit import AggregateKit, BackendRequirements
from .repository import AggregateRepository, aggregate_repository

# ----------------------- #

__all__ = [
    "AggregateKit",
    "AggregateRepository",
    "BackendRequirements",
    "aggregate_repository",
]
