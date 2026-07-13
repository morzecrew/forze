"""Shared security guard forbidding caching/history on credential documents.

Caching or history on credential, session, principal, and identity-mapping
documents would persist secret material or expose soft-deleted records, so both
are forbidden. Centralizing the check keeps the secure default a single call and
prevents a future port author from silently omitting it.
"""

from collections.abc import Callable
from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, exc

# ----------------------- #


def forbid_cache_and_history(
    *specs: DocumentSpec[Any, Any, Any, Any],
    label: str,
    error: Callable[[str], CoreException] = exc.internal,
) -> None:
    """Reject caching or history on security-sensitive identity document specs.

    :param specs: Document specs to validate (e.g. a query and a command spec).
    :param label: Human-readable resource name used in the error message.
    :param error: Exception factory for violations; defaults to :func:`exc.internal`.
    :raises CoreException: If any spec enables caching or history.
    """

    for spec in specs:
        if spec.cache is not None:
            raise error(f"{label} caching is forbidden by security reasons")

    for spec in specs:
        if spec.history_enabled:
            raise error(f"{label} history is forbidden by security reasons")
