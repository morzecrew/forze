"""Type variables shared by document adapter mixins."""

from typing import TypeVar

from pydantic import BaseModel

from forze.domain.models import BaseDTO, Document

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)
T = TypeVar("T", bound=BaseModel)
