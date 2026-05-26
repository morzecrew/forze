from .egress import exception_egress_policy
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
    "ExceptionInterceptor",
    "map_pydantic",
    "default_chain_exc_mapper",
]
