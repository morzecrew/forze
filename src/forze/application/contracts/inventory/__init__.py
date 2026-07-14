"""The spec inventory: what an application binds, and what an export may do with it.

A spec is the only object that knows a plane's portable shape — its models, its encrypted
fields, its materialized set. But specs are passed to ports at *resolve* time and stored
nowhere, so until now nothing could answer "what does this application consist of?". The
dependency registry knows every ``(key, route)`` pair it binds; it does not know a single
spec. The inventory closes that gap, and :func:`reconcile_specs` keeps the two honest about
each other at startup.
"""

from .planes import (
    DEFAULT_DISPOSITIONS,
    PLANE_DEP_KEYS,
    SPEC_TYPE_PLANES,
    disposition_of,
    plane_of_key,
    plane_of_spec,
)
from .reconcile import reconcile_specs
from .refusal import assert_exportable, refusal_reason
from .registry import FrozenSpecRegistry, SpecRegistry, spec_ref
from .value_objects import (
    PlaneDisposition,
    SpecEdge,
    SpecEdgeKind,
    SpecPlane,
    SpecRef,
    SpecRegistryEntry,
    SpecSource,
)

# ----------------------- #

__all__ = [
    "DEFAULT_DISPOSITIONS",
    "PLANE_DEP_KEYS",
    "SPEC_TYPE_PLANES",
    "FrozenSpecRegistry",
    "PlaneDisposition",
    "SpecEdge",
    "SpecEdgeKind",
    "SpecPlane",
    "SpecRef",
    "SpecRegistry",
    "SpecRegistryEntry",
    "SpecSource",
    "assert_exportable",
    "disposition_of",
    "plane_of_key",
    "plane_of_spec",
    "reconcile_specs",
    "refusal_reason",
    "spec_ref",
]
