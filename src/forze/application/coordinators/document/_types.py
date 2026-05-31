"""Type variables shared by document coordinator mixins."""

from typing import TypeVar

from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)
T = TypeVar("T", bound=BaseModel)
