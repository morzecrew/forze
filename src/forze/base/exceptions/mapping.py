from __future__ import annotations

from typing import Any, Mapping, final

import attrs
from pydantic import ValidationError as PydanticValidationError

from ..conformity import static_fn_conformity
from ..descriptors import hybridmethod
from ._utils import default_exception
from .model import CoreException
from .protocols import ExceptionMapper

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ChainExceptionMapper(ExceptionMapper):
    """Chain exception mapper."""

    mappers: tuple[ExceptionMapper, ...]
    """The mappers to chain."""

    fallback: ExceptionMapper | None = None
    """The fallback mapper to use if no mapper matches."""

    # ....................... #

    def __call__(
        self,
        exc: BaseException,
        *,
        site: str,
        details: Mapping[str, Any] | None = None,
    ) -> CoreException | None:
        """Map an exception to a :class:`CoreException`."""

        if isinstance(exc, CoreException):
            return exc

        for mapper in self.mappers:
            res = mapper(exc, site=site, details=details)

            if res is not None:
                return res

        if self.fallback is not None:
            return self.fallback(exc, site=site, details=details)

        return default_exception(exc, site=site)

    # ....................... #

    @hybridmethod
    def chain(
        cls: type[ChainExceptionMapper],  # type: ignore[misc, override]
        *mappers: ExceptionMapper,
        fallback: ExceptionMapper | None = None,
    ) -> ChainExceptionMapper:
        """Construct a chain of exception mappers."""

        return cls(mappers=mappers, fallback=fallback)

    # ....................... #

    @chain.instancemethod  # type: ignore[arg-type]
    def _chain_instance(  # type: ignore[misc, override]
        self: ChainExceptionMapper,
        *mappers: ExceptionMapper,
        fallback: ExceptionMapper | None = None,
    ) -> ChainExceptionMapper:
        """Chain the exception mappers."""

        return type(self).chain(*(self, *mappers), fallback=fallback)


# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def map_pydantic(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Map a :class:`PydanticValidationError` to a :class:`CoreException`."""

    from ..scrubbing import sanitize_pydantic_errors

    _ = site, details

    if not isinstance(exc, PydanticValidationError):
        return None

    return CoreException.validation(
        exc.title or "Validation failed",
        code="pydantic.validation",
        details={"errors": sanitize_pydantic_errors(list(exc.errors()))},
    )


# ....................... #

default_chain_exc_mapper = ChainExceptionMapper.chain(map_pydantic)
"""The default chain exception mapper."""
