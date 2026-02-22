from typing import TypedDict
from uuid import UUID

from forze.domain.models import BaseDTO

from ..public import RawSearchRequestDTO, SearchRequestDTO

# ----------------------- #


class SoftDeleteArgs(TypedDict):
    pk: UUID
    rev: int


# ....................... #


class UpdateArgs[In: BaseDTO](TypedDict):
    pk: UUID
    dto: In
    rev: int


# ....................... #


class SearchArgs(TypedDict):
    body: SearchRequestDTO
    page: int
    size: int


# ....................... #


class RawSearchArgs(TypedDict):
    body: RawSearchRequestDTO
    page: int
    size: int
