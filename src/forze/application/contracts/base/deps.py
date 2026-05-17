from typing import TYPE_CHECKING, Protocol, TypeVar, final

import attrs

from .specs import BaseSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class DepKey[T]:
    """Typed key used to identify dependencies in the kernel.

    The ``name`` is used for diagnostics and error messages; type information
    is carried through the type parameter ``T`` for static resolution.
    """

    name: str
    """Human-readable name for diagnostics and error messages."""


# ....................... #


class BaseDepPort[S: BaseSpec, Port](Protocol):
    """Base protocol for building resource ports."""

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: S,
    ) -> Port: ...  # pragma: no cover
