"""Structured logging facade built on loguru."""

from .facade import configure, getLogger, reset
from .helpers import safe_preview
from .setup import setup_default

# ----------------------- #

__all__ = [
    "configure",
    "getLogger",
    "reset",
    "setup_default",
    "safe_preview",
]
