"""The Postgres error mapper's branches that only run when something has gone wrong.

The engine-facing mappings are pinned against a live server in the integration suite — that is
the only place they mean anything. What is left here are the arms a real server rarely reaches
in a test: the driver rejecting a call before the server sees it, and the deliberate *defer*
for failures that are not this statement's fault. Both are detection code, so both have to be
proved reachable rather than assumed.
"""

from __future__ import annotations

import pytest
from psycopg import errors

from forze.application.integrations.dynamic_read import STATEMENT_INVALID_CODE
from forze.base.exceptions import ExceptionKind
from forze_postgres.adapters.dynamic_read import dynamic_read_error

ROUTE = "widgets"


def _with_sqlstate(kind: type[errors.Error], sqlstate: str, message: str) -> errors.Error:
    """A psycopg error carrying *sqlstate*, without a live server to produce one.

    psycopg reads ``sqlstate`` off the diagnostic the server sent, so a hand-built error has
    none. Overriding the property on a throwaway subclass is what lets the class-based arms be
    exercised without a container.
    """

    return type(kind.__name__, (kind,), {"sqlstate": sqlstate})(message)


def test_a_driver_side_programming_error_is_the_callers_statement() -> None:
    """No SQLSTATE means the server never saw it — a bad placeholder, or a no-row statement.

    The realistic case is a literal ``%`` the author did not double: psycopg refuses to compose
    the query at all, client-side. Reported as infrastructure it would look like the database
    was broken, when the statement simply cannot be sent.
    """

    mapped = dynamic_read_error(
        errors.ProgrammingError("only '%s', '%b', '%t' are allowed as placeholders"),
        route=ROUTE,
    )

    assert mapped is not None
    assert mapped.code == STATEMENT_INVALID_CODE
    assert mapped.kind == ExceptionKind.VALIDATION


def test_a_statement_that_produces_no_result_set_is_refused_as_invalid() -> None:
    """This plane's contract is rows; a statement returning none is mis-authored for it."""

    mapped = dynamic_read_error(
        errors.ProgrammingError("the operation in stream() didn't produce a result"),
        route=ROUTE,
    )

    assert mapped is not None
    assert mapped.code == STATEMENT_INVALID_CODE


@pytest.mark.parametrize(
    "error",
    [
        errors.OperationalError("server closed the connection unexpectedly"),
        _with_sqlstate(errors.OperationalError, "08006", "connection failure"),
        _with_sqlstate(errors.OperationalError, "53300", "too many connections"),
        _with_sqlstate(errors.InternalError, "40001", "serialization failure"),
    ],
    ids=["no-sqlstate-operational", "connection", "too-many-connections", "serialization"],
)
def test_failures_that_are_not_this_statements_fault_are_deferred(
    error: errors.Error,
) -> None:
    """Deferring matters as much as mapping.

    A dead connection, a saturated server and a serialization conflict already have mappings
    callers handle as infrastructure or concurrency — and, crucially, as *retryable*. Claiming
    them here would relabel a transient outage as a permanently invalid statement, and the
    caller would stop retrying something that was about to work.
    """

    assert dynamic_read_error(error, route=ROUTE) is None


def test_a_non_psycopg_exception_is_deferred() -> None:
    """The mapper only speaks for the driver; anything else keeps the mapping it had."""

    assert dynamic_read_error(ValueError("not a database problem"), route=ROUTE) is None


def test_a_data_exception_reports_its_sqlstate_and_not_the_value() -> None:
    """Class 22 names a value, and the value may have come out of a column.

    Pinned here as well as against the live server: this is the arm where a change from
    "SQLSTATE only" back to the server's message would put a tenant's data into an error that
    egresses to the caller.
    """

    error = _with_sqlstate(
        errors.DataError,
        "22P02",
        'invalid input syntax for type integer: "nadia@example.com"',
    )
    mapped = dynamic_read_error(error, route=ROUTE)

    assert mapped is not None
    assert mapped.details is not None
    assert mapped.details["detail"] == "SQLSTATE 22P02"
    assert "nadia@example.com" not in repr(mapped.details)


def test_an_adapter_whose_two_timeouts_disagree_is_refused() -> None:
    """The route ceiling arrives twice; a route where they differ is a construction bug.

    The config is where the author wrote it and the shell field is the copy actually enforced,
    so a mismatch means the wiring documents one ceiling while calls are clamped against
    another. The production factory cannot produce it — hand construction could.
    """

    from datetime import timedelta
    from unittest.mock import Mock

    from forze.application.contracts.dynamic_read import DynamicReadSpec
    from forze.base.exceptions import CoreException
    from forze_postgres.adapters.dynamic_read import PostgresDynamicReadAdapter
    from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig
    from forze_postgres.kernel.client import PostgresClient

    config = PostgresDynamicReadConfig(
        provenance="trusted",
        statement_timeout=timedelta(seconds=5),
    )

    with pytest.raises(CoreException):
        PostgresDynamicReadAdapter(
            client=Mock(spec=PostgresClient),
            spec=DynamicReadSpec(name=ROUTE),
            config=config,
            statement_timeout=timedelta(seconds=30),
        )
