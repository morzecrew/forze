"""FastAPI integration helpers for Forze.

This package provides optional routing primitives, parameter helpers, and
ready-to-use routers that connect FastAPI endpoints to the Forze application
kernel and infrastructure.
"""

from .lifespan import run_startup_checks, runtime_lifespan
from .openapi import apply_openapi_conventions
from .serving import serve_uvicorn

# ----------------------- #

__all__ = [
    "apply_openapi_conventions",
    "run_startup_checks",
    "runtime_lifespan",
    "serve_uvicorn",
]
