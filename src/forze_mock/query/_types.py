"""Query helpers: sentinel and document TypeVars."""

from __future__ import annotations

from typing import (
    TypeVar,
)
from pydantic import BaseModel
from forze.application.contracts.querying.internal.matching import (
    _MISSING,  # type: ignore[reportPrivateUsage]
)
from forze.domain.models import BaseDTO, Document, ReadDocument

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)
M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

# Re-exported from core so the absent-field sentinel is one identity across the matcher (which
# returns it from ``_path_get``) and the mock's sort/projection/cache code that tests ``is _MISSING``.
__all__ = ["_MISSING", "R", "D", "C", "U", "M", "T"]
