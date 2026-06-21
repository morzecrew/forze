from .egress import exception_egress_policy, http_status_for_kind
from .fallback import fallback_exception_mapper
from .http_mapper import make_http_exception_mapper, response_status
from .interceptor import ExceptionInterceptor
from .mapping import ChainExceptionMapper, default_chain_exc_mapper, map_pydantic
from .model import CoreException, ExceptionKind
from .protocols import ExceptionMapper

# ----------------------- #

exc = CoreException
"""Convenience alias for :class:`CoreException`."""

# ....................... #

__all__ = [
    "CoreException",
    "exc",
    "ExceptionKind",
    "ChainExceptionMapper",
    "ExceptionMapper",
    "exception_egress_policy",
    "http_status_for_kind",
    "ExceptionInterceptor",
    "fallback_exception_mapper",
    "make_http_exception_mapper",
    "map_pydantic",
    "default_chain_exc_mapper",
    "response_status",
]
