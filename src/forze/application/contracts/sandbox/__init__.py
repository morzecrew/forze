"""Sandbox contracts: governed out-of-process execution behind one hexagonal seam.

One :class:`SandboxSpec` names one execution route and declares whose code it runs; the
wiring config binds it to a backend and to the ceilings that backend enforces. The plane's
value is not isolation — a Python library cannot provide that — but the three things around
it: a fail-closed gate that refuses untrusted code on an adapter that cannot contain it, a
seam a simulation can cut, and one written-once contract for cancellation, resource kills
and bounded output capture.
"""

from .capabilities import (
    DEFAULT_SANDBOX_CAPABILITIES,
    FULL_SANDBOX_CAPABILITIES,
    MINIMUM_UNTRUSTED_ISOLATION,
    UNDERISOLATED_CODE,
    UNKNOWN_PROVENANCE_CODE,
    UNSUPPORTED_SANDBOX_FEATURE_CODE,
    Isolation,
    SandboxCapabilities,
    contains_untrusted,
    isolation_rank,
    validate_provenance,
    validate_resources,
    validate_stream_supported,
)
from .deps import SandboxDepKey, SandboxDepPort, SandboxDeps
from .ports import BaseSandboxPort, SandboxPort
from .specs import Provenance, SandboxSpec
from .value_objects import (
    CapturedStream,
    Outcome,
    ProgramPayload,
    ResourceRequest,
    ResourceUsage,
    SandboxEvent,
    SandboxRequest,
    SandboxResult,
    StorageKeyName,
)

# ----------------------- #

__all__ = [
    "DEFAULT_SANDBOX_CAPABILITIES",
    "FULL_SANDBOX_CAPABILITIES",
    "MINIMUM_UNTRUSTED_ISOLATION",
    "UNDERISOLATED_CODE",
    "UNKNOWN_PROVENANCE_CODE",
    "UNSUPPORTED_SANDBOX_FEATURE_CODE",
    "BaseSandboxPort",
    "CapturedStream",
    "Isolation",
    "Outcome",
    "Provenance",
    "ProgramPayload",
    "ResourceRequest",
    "ResourceUsage",
    "SandboxCapabilities",
    "SandboxDepKey",
    "SandboxDepPort",
    "SandboxDeps",
    "SandboxEvent",
    "SandboxPort",
    "SandboxRequest",
    "SandboxResult",
    "SandboxSpec",
    "StorageKeyName",
    "contains_untrusted",
    "isolation_rank",
    "validate_provenance",
    "validate_resources",
    "validate_stream_supported",
]
