"""Versioned facts with correction lineage: correct, never overwrite."""

from .dto import CorrectDocumentDTO, FactAsOfDTO, FactIdDTO
from .facades import VersionedFacade, versioned_facade
from .factories import build_versioned_registry
from .handlers import CorrectDocument, FactAsOf, FactHistory
from .operations import VersionedKernelOp
from .policy import (
    ONE_CURRENT_VERSION,
    ONE_SUCCESSOR,
    REQUIRED_GUARANTEES,
    VersionedPolicy,
)
from .wiring import VersionedWiring, current_versions_only_mapper, versioned_wiring

# ----------------------- #

__all__ = [
    "VersionedPolicy",
    "VersionedKernelOp",
    "VersionedWiring",
    "VersionedFacade",
    "CorrectDocument",
    "FactHistory",
    "FactAsOf",
    "CorrectDocumentDTO",
    "FactIdDTO",
    "FactAsOfDTO",
    "build_versioned_registry",
    "versioned_wiring",
    "versioned_facade",
    "current_versions_only_mapper",
    "ONE_CURRENT_VERSION",
    "ONE_SUCCESSOR",
    "REQUIRED_GUARANTEES",
]
