"""Procedures contracts: governed parametrized DB commands & compute (analytics' write twin)."""

from .deps import (
    ProcedureCommandDepKey,
    ProcedureCommandDepPort,
    ProceduresDeps,
)
from .ports import (
    BaseProcedurePort,
    ProcedurePort,
)
from .specs import (
    ProcedureSpec,
    validate_procedure_spec,
)
from .value_objects import ExecResult

# ----------------------- #

__all__ = [
    "BaseProcedurePort",
    "ExecResult",
    "ProcedureCommandDepKey",
    "ProcedureCommandDepPort",
    "ProcedurePort",
    "ProcedureSpec",
    "ProceduresDeps",
    "validate_procedure_spec",
]
