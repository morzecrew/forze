"""Query helpers: sentinel and document TypeVars."""

from __future__ import annotations

from typing import (
    TypeVar,
)
from pydantic import BaseModel
from forze.domain.models import BaseDTO, Document, ReadDocument

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)
M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_MISSING = object()
