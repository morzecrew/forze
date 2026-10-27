"""Dynamic-read contracts: governed execution of runtime-authored read statements."""

from .deps import (
    DynamicReadDepKey,
    DynamicReadDepPort,
    DynamicReadDeps,
)
from .ports import (
    BaseDynamicReadPort,
    DynamicReadPort,
)
from .specs import (
    STATEMENT_CAPTURE_KEY,
    DynamicReadSpec,
    validate_dynamic_read_spec,
)
from .types import DynamicReadOptions
from .value_objects import DynamicReadProvenance, validate_dynamic_read_provenance

# ----------------------- #

__all__ = [
    "STATEMENT_CAPTURE_KEY",
    "BaseDynamicReadPort",
    "DynamicReadDepKey",
    "DynamicReadDepPort",
    "DynamicReadDeps",
    "DynamicReadOptions",
    "DynamicReadPort",
    "DynamicReadProvenance",
    "DynamicReadSpec",
    "validate_dynamic_read_provenance",
    "validate_dynamic_read_spec",
]
