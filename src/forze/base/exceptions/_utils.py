import asyncio
import inspect
from functools import lru_cache
from typing import Any, Callable, Final, Mapping, Never

from .model import CoreException, ExceptionKind
from .protocols import ExceptionMapper

# ----------------------- #

BYPASS_INTERCEPTION: Final[tuple[type[BaseException], ...]] = (
    GeneratorExit,
    KeyboardInterrupt,
    SystemExit,
    asyncio.CancelledError,
)
"""Control-flow exceptions that always pass through interception unmapped."""

# ....................... #


def default_exception(exc: BaseException, site: str) -> CoreException:
    """Create a default exception for an unhandled exception."""

    return CoreException(
        kind=ExceptionKind.INTERNAL,
        summary="Unhandled exception",
        code="core.unhandled",
        details={"site": site, "exc_type": type(exc).__name__},
    )


# ....................... #


def reraise_mapped(
    mapper: ExceptionMapper,
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> Never:
    """Reraise a mapped exception."""

    if isinstance(exc, CoreException):
        raise exc

    if isinstance(exc, BYPASS_INTERCEPTION):
        raise exc

    err = mapper(exc, site=site, details=details)

    if err is None:
        err = default_exception(exc, site)

    # Every mapped exception carries the interception site for observability,
    # even when the package-specific mapper didn't include it itself.
    elif err.details is None:
        err.details = {"site": site}

    elif "site" not in err.details:
        err.details = {**err.details, "site": site}

    raise err from exc


# ....................... #


@lru_cache(maxsize=256)
def cached_signature(fn: Callable[..., Any]) -> inspect.Signature:
    """Cache the signature of a callable."""

    return inspect.signature(fn)


# ....................... #


def resolve_site(fn: Callable[..., Any], site: str | None) -> str:
    """Resolve the internal site, falling back to the callable's ``__name__``."""

    return site or fn.__name__
