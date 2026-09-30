"""FastAPI integration helpers for Forze.

This package provides optional routing primitives, parameter helpers, and
ready-to-use routers that connect FastAPI endpoints to the Forze application
kernel and infrastructure.
"""

from .lifespan import runtime_lifespan
from .openapi import apply_openapi_conventions

# ----------------------- #

__all__ = [
    "apply_openapi_conventions",
    "runtime_lifespan",
]
