from typing import Awaitable, Protocol

# ----------------------- #


class Mapper[In, Out](Protocol):
    """Protocol for a mapper that maps a single source to a single output."""

    def __call__(self, source: In) -> Awaitable[Out]: ...  # noqa: F841
