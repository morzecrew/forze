"""The dynamic-read taxonomy, and its parity with the page that documents it.

On this plane the error code *is* the API: the statement came from another program, so the
only thing a caller can branch on is which refusal came back. Two things therefore have to
stay true — every factory raises the code and kind it advertises, and the docs page lists all
of them. A code that ships undocumented is one a caller cannot handle, which on a plane whose
whole value is governed failure is the same as not shipping it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from forze.application.integrations.dynamic_read import (
    DYNAMIC_READ_CODES,
    MULTI_STATEMENT_CODE,
    PERMISSION_DENIED_CODE,
    ROLE_UNAVAILABLE_CODE,
    ROW_CAP_EXCEEDED_CODE,
    STATEMENT_INVALID_CODE,
    STATEMENT_TOO_LARGE_CODE,
    TIMEOUT_CODE,
    WRITE_REFUSED_CODE,
    multi_statement,
    permission_denied,
    role_unavailable,
    row_cap_exceeded,
    statement_invalid,
    statement_too_large,
    timed_out,
    write_refused,
)
from forze.base.exceptions import CoreException, ExceptionKind

ROUTE = "widgets"

DOCS_PAGE = (
    Path(__file__).resolve().parents[5] / "pages" / "docs" / "data-events" / "dynamic-read.md"
)

BUILT: tuple[tuple[CoreException, str, ExceptionKind], ...] = (
    (write_refused(ROUTE), WRITE_REFUSED_CODE, ExceptionKind.PRECONDITION),
    (statement_invalid(ROUTE), STATEMENT_INVALID_CODE, ExceptionKind.VALIDATION),
    (multi_statement(ROUTE), MULTI_STATEMENT_CODE, ExceptionKind.VALIDATION),
    (permission_denied(ROUTE), PERMISSION_DENIED_CODE, ExceptionKind.PRECONDITION),
    (role_unavailable(ROUTE, role="r"), ROLE_UNAVAILABLE_CODE, ExceptionKind.CONFIGURATION),
    (timed_out(ROUTE), TIMEOUT_CODE, ExceptionKind.TIMEOUT),
    (row_cap_exceeded(ROUTE, row_cap=1), ROW_CAP_EXCEEDED_CODE, ExceptionKind.PRECONDITION),
    (
        statement_too_large(ROUTE, size=2, limit=1),
        STATEMENT_TOO_LARGE_CODE,
        ExceptionKind.VALIDATION,
    ),
)


@pytest.mark.parametrize(
    ("error", "code", "kind"),
    BUILT,
    ids=[code for _, code, _ in BUILT],
)
def test_every_factory_raises_the_code_and_kind_it_advertises(
    error: CoreException,
    code: str,
    kind: ExceptionKind,
) -> None:
    """Kind decides egress — a mislabelled one turns a permanent refusal into a retry loop."""

    assert error.code == code
    assert error.kind == kind
    assert error.details is not None
    assert error.details["route"] == ROUTE


def test_the_code_set_is_exactly_what_the_factories_produce() -> None:
    """``DYNAMIC_READ_CODES`` cannot drift into a stale list of a plane that moved on."""

    assert {code for _, code, _ in BUILT} == set(DYNAMIC_READ_CODES)


def test_nothing_on_this_plane_egresses_as_internal() -> None:
    """The error-code-hygiene rule, for the plane where it bites hardest.

    The statement is the caller's input here, so an undefined relation is a malformed request.
    Reported as ``internal`` it would page whoever owns the database for a typo somebody's
    report builder produced — and it would be retried, because that is what the egress policy
    does with an internal error.
    """

    assert all(error.kind is not ExceptionKind.INTERNAL for error, _, _ in BUILT)


def test_every_code_is_documented() -> None:
    """A code the docs page does not list is one a caller has no way to learn about."""

    page = DOCS_PAGE.read_text(encoding="utf-8")
    missing = sorted(code for code in DYNAMIC_READ_CODES if code not in page)

    assert not missing, (
        f"{DOCS_PAGE.name} does not mention {missing} — add the row to its error table"
    )


def test_the_documented_table_invents_no_codes() -> None:
    """The other direction: a documented code with no factory is a promise nothing keeps."""

    page = DOCS_PAGE.read_text(encoding="utf-8")
    documented = set(re.findall(r"dynamic_read_[a-z_]+", page))
    # ``_row_type_mismatch`` is raised by the shared shell's ``select`` rather than by this
    # module's factories, so it is documented and legitimately absent from the code set.
    documented.discard("dynamic_read_row_type_mismatch")
    # Prose references to wiring guards — configuration refusals raised at freeze by the
    # Postgres deps module, not members of this runtime taxonomy. The container guard has no
    # entry here at all: it is the shared statement-origin floor, whose code carries no
    # ``dynamic_read_`` prefix precisely because it is not this plane's to own.
    documented -= {
        "dynamic_read_untrusted_unconfined",
        "dynamic_read_shared_role_across_tenants",
    }

    assert documented <= set(DYNAMIC_READ_CODES), sorted(documented - set(DYNAMIC_READ_CODES))
