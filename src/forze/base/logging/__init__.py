"""Structured logging facade built on loguru.

This package provides a stdlib-like API for internal use while using loguru
under the hood. It supports:

- **Namespace-based level configuration**: Set different levels per prefix
  (e.g. ``{"forze.application": "DEBUG"}``).
- **Indentation-aware sections**: Use :func:`log_section` to nest log blocks
  for clearer hierarchy.
- **Explicit setup**: No configuration on import. Call :func:`setup_default`
  or :func:`configure` at application startup.
"""

from .context import log_section
from .facade import configure, getLogger, reset
from .helpers import safe_preview
from .setup import setup_default

# ----------------------- #

__all__ = [
    "configure",
    "getLogger",
    "log_section",
    "reset",
    "setup_default",
    "safe_preview",
]
