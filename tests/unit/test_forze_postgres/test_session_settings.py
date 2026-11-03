"""Transaction-local settings: one statement to set them, and to capture the caller's."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from forze_postgres.kernel.client import local_settings_statement, scoped_local_settings

# ----------------------- #


def _sql(query: Any) -> str:
    return query.as_string(None)


class _Client:
    def __init__(self, priors: tuple[str, ...] = ()) -> None:
        self.priors = priors
        self.fetches: list[str] = []
        self.executes: list[tuple[str, Any]] = []

    async def fetch_one(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        self.fetches.append(_sql(query))
        return (*self.priors, "set", "set")

    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
        self.executes.append((_sql(query), params))


# ....................... #


def test_nothing_to_set_is_no_statement() -> None:
    assert local_settings_statement() is None


def test_the_timeout_only_lowers_the_bound_in_force() -> None:
    statement = _sql(local_settings_statement(statement_timeout=timedelta(seconds=5)))

    assert "set_config('statement_timeout', LEAST(5000, NULLIF(" in statement
    assert "current_setting('statement_timeout')" in statement
    assert statement.endswith(", true)")  # transaction-local


def test_the_tenant_schema_comes_before_public() -> None:
    statement = _sql(local_settings_statement(search_path="tenant_a"))

    assert (
        "set_config('search_path', quote_ident('tenant_a') || ', public, pg_temp', true)"
        in statement
    )


@pytest.mark.asyncio
async def test_inside_a_caller_transaction_one_statement_captures_and_sets() -> None:
    client = _Client(priors=("30s", "public"))

    async with scoped_local_settings(
        client,  # type: ignore[arg-type]
        statement_timeout=timedelta(seconds=5),
        search_path="tenant_a",
        restore=True,
    ):
        assert client.executes == []  # nothing besides the one capture-and-set statement

    [capture] = client.fetches
    assert capture.startswith("WITH prior AS MATERIALIZED (SELECT current_setting(")
    assert "set_config('statement_timeout'" in capture
    assert "set_config('search_path'" in capture

    [(restore, params)] = client.executes
    assert params == ["statement_timeout", "30s", "search_path", "public"]
    assert "set_config" in restore


@pytest.mark.asyncio
async def test_the_caller_values_come_back_when_the_body_raises() -> None:
    client = _Client(priors=("30s",))

    with pytest.raises(RuntimeError):
        async with scoped_local_settings(
            client,  # type: ignore[arg-type]
            statement_timeout=timedelta(seconds=5),
            restore=True,
        ):
            raise RuntimeError("the query failed")

    assert [params for _, params in client.executes] == [["statement_timeout", "30s"]]


@pytest.mark.asyncio
async def test_at_the_root_it_sets_and_restores_nothing() -> None:
    client = _Client()

    async with scoped_local_settings(
        client,  # type: ignore[arg-type]
        statement_timeout=timedelta(seconds=5),
        restore=False,
    ):
        pass

    assert client.fetches == []
    assert [params for _, params in client.executes] == [None]
