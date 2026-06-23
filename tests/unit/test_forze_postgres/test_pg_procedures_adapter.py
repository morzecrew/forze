"""Tests for PostgresProcedureAdapter and its config, with a mocked client."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.procedure import ProcedureSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_postgres.adapters.procedure import PostgresProcedureAdapter
from forze_postgres.execution.deps.configs import PostgresProcedureConfig

# ----------------------- #


class _Params(BaseModel):
    window: str = "2026-01-01"


class _RowOut(BaseModel):
    total: int = 0


# ....................... #


class _MockClient:
    def __init__(
        self,
        *,
        value: Any = None,
        row: dict[str, Any] | None = None,
        rowcount: int = 0,
    ) -> None:
        self.value = value
        self.row = row
        self.rowcount = rowcount
        self.executes: list[tuple[Any, Any]] = []
        self.fetch_value_calls: list[tuple[Any, Any]] = []
        self.fetch_one_calls: list[tuple[Any, Any]] = []
        self.tx_opened = 0

    def transaction(self) -> Any:
        @asynccontextmanager
        async def _tx() -> Any:
            self.tx_opened += 1
            yield self

        return _tx()

    async def execute(
        self, query: Any, params: Any = None, *, return_rowcount: bool = False
    ) -> Any:
        self.executes.append((query, params))
        return self.rowcount if return_rowcount else None

    async def fetch_value(self, query: Any, params: Any = None, *, default: Any = None) -> Any:
        self.fetch_value_calls.append((query, params))
        return self.value

    async def fetch_one(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        _ = kwargs
        self.fetch_one_calls.append((query, params))
        return self.row


def _adapter(
    mock: Any,
    *,
    spec: ProcedureSpec[Any, Any],
    config: PostgresProcedureConfig,
    tenant_provider: Any = None,
) -> PostgresProcedureAdapter[Any, Any]:
    return PostgresProcedureAdapter(
        client=mock, spec=spec, config=config, tenant_provider=tenant_provider
    )


# ----------------------- #
# cardinality dispatch


@pytest.mark.asyncio
async def test_side_effect_returns_affected_count() -> None:
    mock = _MockClient(rowcount=7)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=PostgresProcedureConfig(sql="SELECT recompute(%(window)s)"),
    )
    result = await adapter.run(_Params())
    assert result.affected_count == 7
    assert result.value is None
    assert mock.tx_opened == 1  # default in_transaction=True


@pytest.mark.asyncio
async def test_scalar_returns_value() -> None:
    mock = _MockClient(value=42)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="compute", params=_Params, result=int),
        config=PostgresProcedureConfig(sql="SELECT compute(%(window)s)"),
    )
    result = await adapter.run(_Params())
    assert result.value == 42
    assert len(mock.fetch_value_calls) == 1


@pytest.mark.asyncio
async def test_scalar_coerces_text_to_declared_type() -> None:
    # A function returning text "42" for result=int is coerced at the boundary.
    mock = _MockClient(value="42")
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="compute", params=_Params, result=int),
        config=PostgresProcedureConfig(sql="SELECT compute(%(window)s)"),
    )
    result = await adapter.run(_Params())
    assert result.value == 42
    assert isinstance(result.value, int)


@pytest.mark.asyncio
async def test_scalar_rejects_wrong_typed_value() -> None:
    mock = _MockClient(value="oops")
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="compute", params=_Params, result=int),
        config=PostgresProcedureConfig(sql="SELECT compute(%(window)s)"),
    )
    with pytest.raises(CoreException, match="scalar result must be int"):
        await adapter.run(_Params())


@pytest.mark.asyncio
async def test_row_returns_decoded_model() -> None:
    mock = _MockClient(row={"total": 99})
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="compute_row", params=_Params, result=_RowOut),
        config=PostgresProcedureConfig(sql="SELECT total FROM compute_row(%(window)s)"),
    )
    result = await adapter.run(_Params())
    assert isinstance(result.value, _RowOut)
    assert result.value.total == 99


@pytest.mark.asyncio
async def test_row_missing_returns_none() -> None:
    mock = _MockClient(row=None)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="compute_row", params=_Params, result=_RowOut),
        config=PostgresProcedureConfig(sql="SELECT total FROM compute_row(%(window)s)"),
    )
    result = await adapter.run(_Params())
    assert result.value is None


@pytest.mark.asyncio
async def test_params_must_be_spec_type() -> None:
    class _Other(BaseModel):
        x: int = 1

    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="p", params=_Params),
        config=PostgresProcedureConfig(sql="SELECT 1"),
    )
    with pytest.raises(CoreException, match="must be a _Params instance"):
        await adapter.run(_Other())  # type: ignore[arg-type]


# ----------------------- #
# transaction modes


@pytest.mark.asyncio
async def test_autocommit_path_opens_no_transaction() -> None:
    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="refresh", params=_Params),
        config=PostgresProcedureConfig(
            sql="REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
            in_transaction=False,
        ),
    )
    await adapter.run(_Params())
    assert mock.tx_opened == 0


# ----------------------- #
# tenancy


def _tenant_config(**kw: Any) -> PostgresProcedureConfig:
    return PostgresProcedureConfig(
        tenant_aware=True,
        sql="SELECT recompute(%(window)s, %(tenant)s)",
        **kw,
    )


@pytest.mark.asyncio
async def test_tenant_aware_binds_tenant_param() -> None:
    tid = uuid4()
    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=_tenant_config(),
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    await adapter.run(_Params())
    _, params = mock.executes[-1]
    assert params["tenant"] == str(tid)


@pytest.mark.asyncio
async def test_tenant_aware_fails_closed_without_tenant() -> None:
    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=_tenant_config(),
        tenant_provider=lambda: None,
    )
    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.run(_Params())
    assert mock.executes == []


@pytest.mark.asyncio
async def test_query_schema_sets_search_path() -> None:
    tid = uuid4()
    mock = _MockClient(rowcount=1)
    expected = f"tenant_{tid.hex}"
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=PostgresProcedureConfig(
            sql="SELECT recompute(%(window)s)",
            query_schema=lambda t: f"tenant_{t.hex}",
        ),
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    await adapter.run(_Params())
    rendered = [
        q.as_string(None) if hasattr(q, "as_string") else str(q)
        for q, _ in mock.executes
    ]
    search_path = next(s for s in rendered if "search_path" in s)
    assert expected in search_path
    assert "public" in search_path
    assert search_path.index(expected) < search_path.index("public")


# ----------------------- #
# config validation


def test_config_rejects_unscoped_tenant_aware_sql() -> None:
    spec = ProcedureSpec(name="recompute", params=_Params)
    config = PostgresProcedureConfig(tenant_aware=True, sql="SELECT recompute(%(window)s)")
    with pytest.raises(CoreException, match="procedures_tenant_param_unreferenced"):
        config.validate_against_spec(spec)


def test_config_rejects_tenant_param_only_in_comment() -> None:
    # A tenant placeholder that appears only in a comment does not scope the statement.
    spec = ProcedureSpec(name="recompute", params=_Params)
    config = PostgresProcedureConfig(
        tenant_aware=True,
        sql="SELECT recompute_all(%(window)s) -- scope by %(tenant)s",
    )
    with pytest.raises(CoreException, match="procedures_tenant_param_unreferenced"):
        config.validate_against_spec(spec)


def test_config_accepts_real_tenant_use_alongside_comment() -> None:
    spec = ProcedureSpec(name="recompute", params=_Params)
    config = PostgresProcedureConfig(
        tenant_aware=True,
        sql="SELECT recompute(%(window)s, %(tenant)s) /* scoped */",
    )
    config.validate_against_spec(spec)  # does not raise


def test_config_allows_namespace_tier_without_tenant_param() -> None:
    # A per-tenant query_schema scopes by schema, so %(tenant)s is not required.
    spec = ProcedureSpec(name="recompute", params=_Params)
    config = PostgresProcedureConfig(
        tenant_aware=True,
        sql="REFRESH MATERIALIZED VIEW region_totals",
        query_schema=lambda t: f"tenant_{t.hex}",
    )
    config.validate_against_spec(spec)  # does not raise


def test_config_static_schema_still_requires_tenant_param() -> None:
    # A static (non-per-tenant) schema does not isolate, so the placeholder is still required.
    spec = ProcedureSpec(name="recompute", params=_Params)
    config = PostgresProcedureConfig(
        tenant_aware=True,
        sql="SELECT recompute(%(window)s)",
        query_schema="fixed_schema",
    )
    with pytest.raises(CoreException, match="procedures_tenant_param_unreferenced"):
        config.validate_against_spec(spec)


def test_config_autocommit_with_timeout_rejected() -> None:
    with pytest.raises(CoreException, match="procedures_autocommit_timeout"):
        PostgresProcedureConfig(
            sql="REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
            in_transaction=False,
            statement_timeout=timedelta(seconds=5),
        )


def test_config_autocommit_with_schema_rejected() -> None:
    with pytest.raises(CoreException, match="procedures_autocommit_schema"):
        PostgresProcedureConfig(
            sql="REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
            in_transaction=False,
            query_schema="tenant_x",
        )


def test_config_empty_sql_rejected() -> None:
    with pytest.raises(CoreException, match="non-empty"):
        PostgresProcedureConfig(sql="   ")


# ----------------------- #
# deps wiring


def test_deps_module_registers_procedure_command_key() -> None:
    from tests.support.execution_context import context_from_deps

    from forze_postgres import PostgresClient, PostgresDepsModule

    module = PostgresDepsModule(
        client=PostgresClient(),
        procedures={
            "recompute": PostgresProcedureConfig(sql="SELECT recompute(%(window)s)"),
        },
    )
    ctx = context_from_deps(module())
    assert ctx.procedure.command(ProcedureSpec(name="recompute", params=_Params)) is not None


def test_required_dedicated_isolation_rejects_shared_client_with_procedure() -> None:
    from forze_postgres import PostgresClient, PostgresDepsModule

    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        PostgresDepsModule(
            client=PostgresClient(),
            required_tenant_isolation="dedicated",
            procedures={
                "recompute": PostgresProcedureConfig(
                    tenant_aware=True,
                    sql="SELECT recompute(%(window)s, %(tenant)s)",
                ),
            },
        )
