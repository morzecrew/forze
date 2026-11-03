"""Tests for PostgresProcedureAdapter and its config, with a mocked client."""

from __future__ import annotations

import asyncio
import base64
import re
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.procedure import ProcedureSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.procedure import resolve_procedure_codecs_spec
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement
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
        in_caller_tx: bool = False,
        session_settings: dict[str, str] | None = None,
    ) -> None:
        self.value = value
        self.row = row
        self.rowcount = rowcount
        self.executes: list[tuple[Any, Any]] = []
        self.fetch_value_calls: list[tuple[Any, Any]] = []
        self.fetch_one_calls: list[tuple[Any, Any]] = []
        self.tx_opened = 0
        # A caller transaction is already open (the normal command-handler case): the adapter's
        # own `transaction()` is then a savepoint, whose SET LOCALs merge into the outer tx.
        self.in_caller_tx = in_caller_tx
        # The caller transaction's own GUC values, as `current_setting` would report them.
        self.session_settings = session_settings or {}
        self.captured: list[Any] = []
        self.conn = _FakeConnection()
        self.bound = 0

    def transaction(self) -> Any:
        @asynccontextmanager
        async def _tx() -> Any:
            self.tx_opened += 1
            yield self

        return _tx()

    def is_in_transaction(self) -> bool:
        return self.in_caller_tx

    @asynccontextmanager
    async def detached(self) -> Any:
        yield

    @asynccontextmanager
    async def bound_connection(self) -> Any:
        self.bound += 1
        yield self.conn

    async def execute(
        self, query: Any, params: Any = None, *, return_rowcount: bool = False
    ) -> Any:
        self.executes.append((query, params))
        return self.rowcount if return_rowcount else None

    async def fetch_value(self, query: Any, params: Any = None, *, default: Any = None) -> Any:
        self.fetch_value_calls.append((query, params))
        return self.value

    async def fetch_one(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        if kwargs.get("row_factory") == "tuple":
            # The capture-and-set statement: one prior per setting it reads.
            names = re.findall(r"current_setting\('(\w+)', true\) AS", query.as_string(None))
            self.captured.append(names)
            return tuple(self.session_settings.get(name) for name in names)

        self.fetch_one_calls.append((query, params))
        return self.row


class _FakeConnection:
    """The pooled connection an autocommit route pins."""

    def __init__(self) -> None:
        self.autocommit = False
        self.closed = False

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value

    async def close(self) -> None:
        self.closed = True


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
async def test_scalar_null_passes_through() -> None:
    # A scalar function returning SQL NULL surfaces as value=None (no coercion attempt).
    mock = _MockClient(value=None)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="compute", params=_Params, result=int),
        config=PostgresProcedureConfig(sql="SELECT compute(%(window)s)"),
    )
    result = await adapter.run(_Params())
    assert result.value is None


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


def _autocommit(mock: Any, timeout: timedelta = timedelta(milliseconds=50)) -> Any:
    return _adapter(
        mock,
        spec=ProcedureSpec(name="refresh", params=_Params),
        config=PostgresProcedureConfig(
            sql="REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
            in_transaction=False,
            statement_timeout=timeout,
        ),
    )


@pytest.mark.asyncio
async def test_autocommit_path_runs_on_its_own_autocommit_connection() -> None:
    class _Seen(_MockClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            assert self.conn.autocommit
            return await super().execute(query, params, **kwargs)

    mock = _Seen(rowcount=3)

    result = await _autocommit(mock, timedelta(seconds=5)).run(_Params())

    assert result.affected_count == 3
    assert mock.bound == 1
    assert mock.conn.autocommit is False  # restored before it goes back to the pool
    assert mock.conn.closed is False


@pytest.mark.asyncio
async def test_autocommit_timeout_with_a_clean_cancel_keeps_the_connection() -> None:
    class _Slow(_MockClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            await asyncio.sleep(10)  # psycopg cancels, drains, and re-raises the cancel

    mock = _Slow()

    with pytest.raises(CoreException, match="canceled") as caught:
        await _autocommit(mock).run(_Params())

    assert caught.value.kind == "infrastructure"
    assert mock.conn.closed is False


@pytest.mark.asyncio
async def test_autocommit_timeout_closes_the_connection_when_the_cancel_does_not_land(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forze_postgres.adapters.procedure import adapter as module

    monkeypatch.setattr(module, "CANCEL_GRACE", timedelta(milliseconds=50))

    class _Stuck(_MockClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            try:
                await asyncio.sleep(10)

            except asyncio.CancelledError:
                # The cancel request got no answer: psycopg's drain waits on the statement.
                await asyncio.sleep(10)

    mock = _Stuck()
    loop = asyncio.get_running_loop()
    started = loop.time()

    with pytest.raises(CoreException, match="canceled") as caught:
        await _autocommit(mock).run(_Params())

    assert caught.value.kind == "infrastructure"
    assert mock.conn.closed is True  # the pool discards it instead of lending it out
    assert loop.time() - started < 1  # timeout + grace, not the statement's 10 s


@pytest.mark.asyncio
async def test_autocommit_timeout_keeps_its_mapping_when_the_drain_fails() -> None:
    class _DrainFails(_MockClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            try:
                await asyncio.sleep(10)

            except asyncio.CancelledError:
                raise OSError("connection reset during the drain") from None

    mock = _DrainFails()

    with pytest.raises(CoreException, match="canceled") as caught:
        await _autocommit(mock).run(_Params())

    assert caught.value.kind == "infrastructure"
    assert isinstance(caught.value.__cause__, OSError)
    assert mock.conn.closed is True


@pytest.mark.asyncio
async def test_autocommit_statement_error_within_the_timeout_passes_through() -> None:
    class _Raises(_MockClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            raise TimeoutError("the pool, not the route")

    mock = _Raises()

    with pytest.raises(TimeoutError, match="the pool"):
        await _autocommit(mock, timedelta(seconds=5)).run(_Params())

    assert mock.conn.closed is False


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


@pytest.mark.asyncio
async def test_tenant_aware_query_schema_resolves_with_bound_tenant() -> None:
    # tenant_aware + a per-tenant query_schema resolves the schema off the bound tenant.
    tid = uuid4()
    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=PostgresProcedureConfig(
            tenant_aware=True,
            sql="REFRESH MATERIALIZED VIEW region_totals",
            query_schema=lambda t: f"tenant_{t.hex}",
        ),
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    await adapter.run(_Params())
    rendered = [
        q.as_string(None) if hasattr(q, "as_string") else str(q)
        for q, _ in mock.executes
    ]
    assert any(f"tenant_{tid.hex}" in s for s in rendered if "search_path" in s)


@pytest.mark.asyncio
async def test_tenant_aware_missing_provider_fails_closed() -> None:
    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=_tenant_config(),
        tenant_provider=None,
    )
    with pytest.raises(CoreException, match="procedures_tenant_provider_missing"):
        await adapter.run(_Params())


def test_tenant_id_for_resolve_missing_provider() -> None:
    adapter = _adapter(
        _MockClient(rowcount=1),
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=_tenant_config(query_schema=lambda t: f"tenant_{t.hex}"),
        tenant_provider=None,
    )
    with pytest.raises(CoreException, match="procedures_tenant_provider_missing"):
        adapter._tenant_id_for_resolve()


def test_tenant_id_for_resolve_unbound_tenant() -> None:
    adapter = _adapter(
        _MockClient(rowcount=1),
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=_tenant_config(query_schema=lambda t: f"tenant_{t.hex}"),
        tenant_provider=lambda: None,
    )
    with pytest.raises(CoreException, match="tenant_required"):
        adapter._tenant_id_for_resolve()


# ----------------------- #
# statement timeout + encrypted params


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured", "expected"),
    [({}, "LEAST(60000,"), ({"statement_timeout": timedelta(seconds=5)}, "LEAST(5000,")],
)
async def test_statement_timeout_tightens_the_transaction_bound(
    configured: dict[str, Any], expected: str
) -> None:
    mock = _MockClient(rowcount=1)
    adapter = _adapter(
        mock,
        spec=ProcedureSpec(name="recompute", params=_Params),
        config=PostgresProcedureConfig(sql="SELECT recompute(%(window)s)", **configured),
    )
    await adapter.run(_Params())
    rendered = [
        q.as_string(None) if hasattr(q, "as_string") else str(q)
        for q, _ in mock.executes
    ]
    [statement] = [s for s in rendered if "set_config('statement_timeout'" in s]
    assert expected in statement
    assert "current_setting('statement_timeout')" in statement  # never loosens


class TestSetLocalDoesNotLeakIntoTheCallerTransaction:
    """A procedure's ``SET LOCAL`` must not outlive the procedure.

    Run inside an open operation transaction — the normal command-handler case — the adapter's
    own ``transaction()`` is a savepoint, and Postgres merges a savepoint's ``SET LOCAL`` values
    into the enclosing transaction when the savepoint is released. So the procedure's per-tenant
    ``search_path`` and its ``statement_timeout`` would silently apply to every statement the
    caller runs afterwards — a tenancy-isolation leak. The caller's values are captured before
    the overrides and put back after (the analytics adapter's SET LOCAL scope, which this seam
    shares).
    """

    @staticmethod
    def _config() -> PostgresProcedureConfig:
        return PostgresProcedureConfig(
            sql="SELECT recompute(%(window)s)",
            statement_timeout=timedelta(seconds=5),
            query_schema="tenant_a",
        )

    @staticmethod
    def _rendered(mock: _MockClient) -> list[str]:
        return [
            q.as_string(None) if hasattr(q, "as_string") else str(q)
            for q, _ in mock.executes
        ]

    @pytest.mark.asyncio
    async def test_caller_values_are_captured_and_restored(self) -> None:
        mock = _MockClient(
            rowcount=1,
            in_caller_tx=True,
            session_settings={
                "search_path": "public",
                "statement_timeout": "30s",
            },
        )
        adapter = _adapter(
            mock,
            spec=ProcedureSpec(name="recompute", params=_Params),
            config=self._config(),
        )

        await adapter.run(_Params())

        # Both overridden settings were captured before being overridden...
        assert mock.captured == [["statement_timeout", "search_path"]]

        # ...and restored to the caller's own values after the procedure ran.
        restore_query, restore_params = mock.executes[-1]
        assert "set_config" in self._rendered(mock)[-1]
        assert restore_params == [
            "statement_timeout",
            "30s",
            "search_path",
            "public",
        ]
        assert restore_query is not None

    @pytest.mark.asyncio
    async def test_at_the_root_nothing_is_captured_or_restored(self) -> None:
        # No caller transaction: the adapter's own transaction ends with the statement and takes
        # the SET LOCAL values with it, so the capture/restore round-trips are skipped.
        mock = _MockClient(rowcount=1, in_caller_tx=False)
        adapter = _adapter(
            mock,
            spec=ProcedureSpec(name="recompute", params=_Params),
            config=self._config(),
        )

        await adapter.run(_Params())

        assert mock.captured == []
        # The timeout is the only statement besides the call; a restore would bind priors.
        assert [params for _, params in mock.executes if params is not None] == [
            {"window": "2026-01-01"}
        ]

    @pytest.mark.asyncio
    async def test_restores_even_when_the_procedure_raises(self) -> None:
        class _Boom(_MockClient):
            async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
                await super().execute(query, params, **kwargs)

                if kwargs.get("return_rowcount"):
                    raise RuntimeError("procedure blew up")

                return None

        mock = _Boom(
            in_caller_tx=True,
            session_settings={"search_path": "public", "statement_timeout": "30s"},
        )
        adapter = _adapter(
            mock,
            spec=ProcedureSpec(name="recompute", params=_Params),
            config=self._config(),
        )

        with pytest.raises(RuntimeError):
            await adapter.run(_Params())

        # A failing procedure must not leave its settings behind either.
        assert any("set_config" in s for s in self._rendered(mock))


# ....................... #


class _EncParams(BaseModel):
    secret: str = "x"
    window: str = "2026-01-01"


@pytest.mark.asyncio
async def test_encrypted_params_sealed_before_bind() -> None:
    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    spec = resolve_procedure_codecs_spec(
        ProcedureSpec(
            name="recompute",
            params=_EncParams,
            encryption=FieldEncryption(encrypted=frozenset({"secret"})),
        ),
        keyring=keyring,
        deterministic=None,
        tenant_provider=lambda: None,
    )
    mock = _MockClient(rowcount=1)
    adapter = PostgresProcedureAdapter(
        client=mock,
        spec=spec,
        config=PostgresProcedureConfig(sql="SELECT recompute(%(secret)s)"),
    )
    await adapter.run(_EncParams(secret="ssn"))
    _, params = mock.executes[-1]
    assert is_envelope(base64.b64decode(params["secret"]))  # sealed before bind
    assert params["window"] == "2026-01-01"


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


def test_config_accepts_tenant_param_after_quoted_double_dash() -> None:
    # A `--` inside a string literal must not be treated as a comment that swallows the real bind.
    spec = ProcedureSpec(name="recompute", params=_Params)
    config = PostgresProcedureConfig(
        tenant_aware=True,
        sql="SELECT recompute(%(window)s) WHERE note = '-- note' AND tenant_id = %(tenant)s",
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


def test_config_autocommit_accepts_a_timeout() -> None:
    config = PostgresProcedureConfig(
        sql="REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
        in_transaction=False,
        statement_timeout=timedelta(minutes=10),
    )

    assert config.statement_timeout == timedelta(minutes=10)


@pytest.mark.parametrize(
    "bad",
    [timedelta(0), timedelta(seconds=-1), None, 60, timedelta(days=30), timedelta(microseconds=1)],
)
def test_config_refuses_a_timeout_postgres_cannot_hold(bad: object) -> None:
    with pytest.raises(CoreException) as caught:
        PostgresProcedureConfig(sql="SELECT 1", statement_timeout=bad)  # type: ignore[arg-type]

    assert caught.value.code == "procedures_timeout_invalid"


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
    from forze_postgres import PostgresClient, PostgresDepsModule
    from tests.support.execution_context import context_from_deps

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
