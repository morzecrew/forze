"""Storage guarantees: what a spec requires of its store, and what an adapter can enforce."""

from .capabilities import (
    FULL_STORAGE_GUARANTEES,
    GUARANTEE_UNSUPPORTED,
    GuaranteeDeclaring,
    GuaranteeEnforcing,
    StorageGuaranteeCapabilities,
    capabilities_of,
    guarantees_of,
    validate_storage_guarantees,
)
from .value_objects import (
    GuaranteeKind,
    NonOverlapping,
    StorageGuarantee,
    StorageGuarantees,
    UniqueTogether,
)

# ----------------------- #

__all__ = [
    "FULL_STORAGE_GUARANTEES",
    "GUARANTEE_UNSUPPORTED",
    "GuaranteeDeclaring",
    "GuaranteeEnforcing",
    "GuaranteeKind",
    "NonOverlapping",
    "StorageGuarantee",
    "StorageGuaranteeCapabilities",
    "StorageGuarantees",
    "UniqueTogether",
    "capabilities_of",
    "guarantees_of",
    "validate_storage_guarantees",
]
