"""FastAPI integration helpers for Forze.

This package provides optional routing primitives, parameter helpers, and
ready-to-use routers that connect FastAPI endpoints to the Forze application
kernel and infrastructure.
"""

from .lifespan import RuntimeLifespan, run_startup_checks, runtime_lifespan
from .openapi import apply_openapi_conventions
from .serving import drain_ahead_of_uvicorn, serve_uvicorn

# ----------------------- #

__all__ = [
    "RuntimeLifespan",
    "apply_openapi_conventions",
    "drain_ahead_of_uvicorn",
    "run_startup_checks",
    "runtime_lifespan",
    "serve_uvicorn",
]
