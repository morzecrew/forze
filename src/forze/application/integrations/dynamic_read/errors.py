"""The dynamic-read error taxonomy, shared by every adapter of the plane.

Every code here is **caller-caused** — a statement the caller compiled did something the
engine refused, or asked for more than the route allows. None of them is ``internal``: on this
plane the statement *is* the caller's input, so a syntax error or a missing relation is the
exact analogue of a malformed request body, not of a broken framework.

The factories live in one module so the mock and a real engine cannot drift into raising
different kinds for the same refusal — the differential compares codes, and a taxonomy that
differs per backend would make the comparison meaningless.
"""

from typing import Any

from forze.base.exceptions import CoreException

# ----------------------- #

WRITE_REFUSED_CODE = "dynamic_read_write_refused"
"""The statement attempted a write or DDL inside the read-only transaction (SQLSTATE 25006)."""

STATEMENT_INVALID_CODE = "dynamic_read_statement_invalid"
"""Syntax error, undefined relation/column/function — the statement itself is wrong."""

MULTI_STATEMENT_CODE = "dynamic_read_multi_statement"
"""The engine rejected a multi-command string; one call executes one statement."""

PERMISSION_DENIED_CODE = "dynamic_read_permission_denied"
"""The confinement refused the statement's target (SQLSTATE 42501).

Distinct from :data:`STATEMENT_INVALID_CODE` on purpose. A cross-schema read blocked by the
route's role is the confinement doing its job, and it is the one refusal an operator wants to
find in a log without it being indistinguishable from a typo."""

TIMEOUT_CODE = "dynamic_read_timeout"
"""The route's statement timeout fired (SQLSTATE 57014)."""

ROW_CAP_EXCEEDED_CODE = "dynamic_read_row_cap_exceeded"
"""The result exceeded the effective row cap; no truncated result is ever returned."""

STATEMENT_TOO_LARGE_CODE = "dynamic_read_statement_too_large"
"""The statement is longer than the route's ``max_statement_bytes``."""

# ....................... #


def _details(route: str, **extra: Any) -> dict[str, Any]:
    return {"route": route, **extra}


# ....................... #


def write_refused(route: str) -> CoreException:
    """A write/DDL refused by the read-only transaction."""

    return CoreException.precondition(
        "Dynamic read refused a statement that attempts to write: the plane runs every "
        "statement in a READ ONLY transaction.",
        code=WRITE_REFUSED_CODE,
        details=_details(route),
    )


def statement_invalid(route: str, *, detail: str | None = None) -> CoreException:
    """A statement the engine could not run (syntax, unknown relation/column/function)."""

    return CoreException.validation(
        "Dynamic read statement is invalid.",
        code=STATEMENT_INVALID_CODE,
        details=_details(route, **({"detail": detail} if detail else {})),
    )


def multi_statement(route: str) -> CoreException:
    """A multi-command string rejected by the engine."""

    return CoreException.validation(
        "Dynamic read executes exactly one statement per call; the engine rejected a "
        "multi-command string.",
        code=MULTI_STATEMENT_CODE,
        details=_details(route),
    )


def permission_denied(route: str, *, detail: str | None = None) -> CoreException:
    """A statement refused by the route's confinement (role grants)."""

    return CoreException.precondition(
        "Dynamic read statement was refused by the route's confinement.",
        code=PERMISSION_DENIED_CODE,
        details=_details(route, **({"detail": detail} if detail else {})),
    )


def timed_out(route: str) -> CoreException:
    """The statement timeout fired."""

    return CoreException.timeout(
        "Dynamic read statement exceeded the route's statement timeout.",
        code=TIMEOUT_CODE,
        details=_details(route),
    )


def row_cap_exceeded(route: str, *, row_cap: int) -> CoreException:
    """The result would have exceeded the effective row cap."""

    return CoreException.precondition(
        "Dynamic read statement returned more rows than the route's cap allows. The result "
        "is refused rather than truncated: a truncated page reads as a complete one.",
        code=ROW_CAP_EXCEEDED_CODE,
        details=_details(route, row_cap=row_cap),
    )


def statement_too_large(route: str, *, size: int, limit: int) -> CoreException:
    """The statement is longer than the route allows."""

    return CoreException.validation(
        "Dynamic read statement exceeds the route's max_statement_bytes.",
        code=STATEMENT_TOO_LARGE_CODE,
        details=_details(route, size=size, limit=limit),
    )


# ....................... #

DYNAMIC_READ_CODES: frozenset[str] = frozenset(
    {
        WRITE_REFUSED_CODE,
        STATEMENT_INVALID_CODE,
        MULTI_STATEMENT_CODE,
        PERMISSION_DENIED_CODE,
        TIMEOUT_CODE,
        ROW_CAP_EXCEEDED_CODE,
        STATEMENT_TOO_LARGE_CODE,
    }
)
"""Every code this plane raises."""
