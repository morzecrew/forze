"""Compose the standard backend exception interceptor from per-backend arms."""

from .fallback import fallback_exception_mapper
from .interceptor import ExceptionInterceptor
from .mapping import default_chain_exc_mapper
from .protocols import ExceptionMapper

# ----------------------- #


def build_exc_interceptor(backend: str, *arms: ExceptionMapper) -> ExceptionInterceptor:
    """Build the standard backend ``ExceptionInterceptor`` from per-backend mapper arms.

    Wraps the scaffold every integration's client error mapper repeats: the default chain
    (``CoreException`` passthrough + pydantic) in front of *arms*, terminated by a
    :func:`fallback_exception_mapper` for *backend* so an unrecognized driver error still
    maps to a sanitized infrastructure error. Each arm handles only its own cases and
    returns ``None`` to defer; the chain consults them in order, then the fallback.
    """

    return ExceptionInterceptor(
        mapper=default_chain_exc_mapper.chain(*arms, fallback=fallback_exception_mapper(backend))
    )
