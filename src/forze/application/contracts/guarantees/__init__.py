"""Storage guarantees: what a spec requires of its store, and what an adapter can enforce."""

from .capabilities import (
    FULL_STORAGE_GUARANTEES,
    GUARANTEE_UNSUPPORTED,
    GuaranteeDeclaring,
    GuaranteeEnforcing,
    GuaranteeImplying,
    StorageGuaranteeCapabilities,
    capabilities_of,
    guarantees_of,
    implied_guarantees_of,
    validate_storage_guarantees,
)
from .value_objects import (
    GuaranteeKind,
    GuaranteeMoment,
    NonOverlapping,
    SerializedBy,
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
    "GuaranteeImplying",
    "GuaranteeKind",
    "GuaranteeMoment",
    "NonOverlapping",
    "SerializedBy",
    "StorageGuarantee",
    "StorageGuaranteeCapabilities",
    "StorageGuarantees",
    "UniqueTogether",
    "capabilities_of",
    "guarantees_of",
    "implied_guarantees_of",
    "validate_storage_guarantees",
]
