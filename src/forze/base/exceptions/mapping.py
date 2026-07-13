from __future__ import annotations

from collections.abc import Mapping
from typing import Any, final

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
        """Construct a chain of exception mappers.

        Nested :class:`ChainExceptionMapper` instances are flattened: their
        mappers are spliced in place (order preserved) and their fallback is
        applied only at the outermost level (an explicit ``fallback`` wins;
        otherwise the first nested fallback is inherited).

        Flattening is required for correctness: a nested chain used as a
        sub-mapper never returns ``None`` — its ``__call__`` falls through to
        ``default_exception`` — so every mapper chained after it would be
        dead code (e.g. ``default_chain_exc_mapper.chain(pkg_mapper)`` would
        never consult ``pkg_mapper``).
        """

        flat: list[ExceptionMapper] = []
        inherited: ExceptionMapper | None = None

        def _flatten(items: tuple[ExceptionMapper, ...]) -> None:
            nonlocal inherited

            for mapper in items:
                if isinstance(mapper, ChainExceptionMapper):
                    if inherited is None:
                        inherited = mapper.fallback

                    _flatten(mapper.mappers)

                else:
                    flat.append(mapper)

        _flatten(mappers)

        return cls(
            mappers=tuple(flat),
            fallback=fallback if fallback is not None else inherited,
        )

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
def map_pydantic(  # skipcq: PY-R1000
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
