from collections.abc import Awaitable
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


class Mapper[In, Out](Protocol):
    """Protocol for a mapper that maps a single source to a single output."""

    def __call__(self, source: In) -> Awaitable[Out]: ...


# ....................... #


class MapperFactory[In, Out](Protocol):
    """Protocol for a factory that builds a mapper."""

    def __call__(self, ctx: "ExecutionContext") -> Mapper[In, Out]: ...
