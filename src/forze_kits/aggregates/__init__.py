"""Aggregate operation kits: registries, facades, and kernel operation ids."""

from .child_collection import ChildCollectionChanges, ChildCollectionSync
from .kit import AggregateKit, BackendRequirements
from .repository import AggregateRepository, aggregate_repository

# ----------------------- #

__all__ = [
    "AggregateKit",
    "AggregateRepository",
    "BackendRequirements",
    "ChildCollectionChanges",
    "ChildCollectionSync",
    "aggregate_repository",
]
