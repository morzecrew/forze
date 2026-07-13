from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from .model import CoreException

# ----------------------- #


class ExceptionMapper(Protocol):
    """Protocol for mapping exceptions to :class:`CoreException`."""

    def __call__(
        self,
        exc: BaseException,
        *,
        site: str,
        details: Mapping[str, Any] | None = None,
    ) -> "CoreException | None": ...
