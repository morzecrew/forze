"""What a failed ``COPY`` hands a client: neither path carries the load's context out."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest

pytest.importorskip("psycopg")

from psycopg import errors

from forze.base.exceptions import CoreException, ExceptionKind, error_envelope, exc
from forze_postgres.kernel.client import client as client_module
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #


def _failing_with(error: Exception) -> Any:
    @asynccontextmanager
    async def statement_conn(self: PostgresClient) -> AsyncGenerator[Any]:
        raise error
        yield  # pragma: no cover

    return statement_conn


async def _copy(monkeypatch: pytest.MonkeyPatch, error: Exception) -> CoreException:
    monkeypatch.setattr(PostgresClient, "_statement_conn", _failing_with(error))

    with pytest.raises(CoreException) as raised:
        await PostgresClient().copy_rows(("app", "events"), ["id"], [(1,)])

    return raised.value


@pytest.mark.asyncio
async def test_a_failure_outside_the_rows_maps_through_the_shared_mapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raised = await _copy(monkeypatch, errors.UniqueViolation("duplicate key"))

    assert raised.kind == ExceptionKind.CONFLICT
    assert raised.details is not None
    assert raised.details["table"] == "events"
    assert raised.details["site"] == "postgres.copy_rows"
    assert error_envelope(raised).context is None


@pytest.mark.asyncio
async def test_a_rejected_row_keeps_the_servers_message_from_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rejected = exc.validation(
        "COPY rejected an input row.",
        code="copy_row_invalid",
        details={"detail": 'invalid input syntax for type integer: "x"', "line": 1},
    )
    monkeypatch.setattr(client_module, "copy_data_error", lambda _e, *, binary: rejected)

    raised = await _copy(monkeypatch, errors.DataError("bad row"))

    assert raised is rejected
    assert raised.details is not None
    assert error_envelope(raised).context is None
