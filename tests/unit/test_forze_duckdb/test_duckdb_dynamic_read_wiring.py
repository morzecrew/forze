"""DuckDB dynamic reads: the wiring refusals at freeze, the config, and the taxonomy mapping.

Battery item 2 of RFC 0078 §7 lives here (with the construction refusals in
``test_duckdb_routed_client.py``); the mapping table is §4.5 as amended by EXECUTION-LOG D-029.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

import duckdb
import pytest

from forze.application.contracts.dynamic_read import DynamicReadSpec
from forze.application.contracts.secrets import SecretRef
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    PERMISSION_DENIED_CODE,
    STATEMENT_INVALID_CODE,
    TIMEOUT_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbDepsModule,
    DuckDbDynamicReadConfig,
    DuckDbQueryConfig,
    LakeTenantCatalog,
    RoutedDuckDbClient,
)
from forze_duckdb.adapters import DuckDbDynamicReadAdapter, dynamic_read_error
from forze_duckdb.kernel.client import QUERY_TIMEOUT_CODE

ROUTE = "widgets"

# ----------------------- #


def _routed(http_timeout: timedelta = timedelta(seconds=5)) -> RoutedDuckDbClient:
    return RoutedDuckDbClient(
        secrets=Any,  # type: ignore[arg-type]  # never resolved: no instance is built here
        secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
        tenant_provider=lambda: None,
        catalog_for_tenant=lambda tenant: LakeTenantCatalog(
            uri="u", warehouse="w", oauth2_server_uri="o", oauth2_scope="s",
            storage_prefix="s3://lake/t/",
        ),
        http_timeout=http_timeout,
    )


def _config(**overrides: Any) -> DuckDbDynamicReadConfig:
    return DuckDbDynamicReadConfig(**{"provenance": "trusted", "namespace": "gold", **overrides})


# ....................... #


class TestWiring:
    def test_a_dynamic_route_on_the_shared_client_is_refused_by_the_origin_floor(self) -> None:
        with pytest.raises(CoreException) as ei:
            DuckDbDepsModule(client=DuckDbClient(), dynamic_reads={ROUTE: _config()})

        assert ei.value.code == "statement_origin_isolation_floor"
        assert ei.value.details["origin"] == "compiled"

    @pytest.mark.parametrize("routed", [True, False])
    def test_untrusted_provenance_is_refused_on_any_client(self, routed: bool) -> None:
        client = _routed() if routed else DuckDbClient()

        with pytest.raises(CoreException) as ei:
            DuckDbDepsModule(client=client, dynamic_reads={ROUTE: _config(provenance="untrusted")})

        assert ei.value.code == "duckdb_dynamic_read_untrusted_refused"
        assert ei.value.details == {"routes": [ROUTE]}

    def test_a_routed_module_derives_dedicated(self) -> None:
        DuckDbDepsModule(
            client=_routed(),
            required_tenant_isolation="dedicated",
            dynamic_reads={ROUTE: _config()},
            analytics={
                "events": DuckDbAnalyticsConfig(
                    queries={"by_day": DuckDbQueryConfig(sql="SELECT 1")}
                )
            },
        )

    def test_http_timeout_above_the_smallest_route_timeout_is_refused(self) -> None:
        reads = {
            "slow": _config(statement_timeout=timedelta(seconds=30)),
            "fast": _config(statement_timeout=timedelta(seconds=2)),
        }

        with pytest.raises(CoreException) as ei:
            DuckDbDepsModule(client=_routed(timedelta(seconds=3)), dynamic_reads=reads)

        assert ei.value.code == "duckdb_http_timeout_above_route_timeout"
        DuckDbDepsModule(client=_routed(timedelta(seconds=2)), dynamic_reads=reads)

    def test_the_wired_route_builds_an_adapter_carrying_the_config(self) -> None:
        client = _routed(timedelta(seconds=3))
        config = _config(statement_timeout=timedelta(seconds=3))
        module = DuckDbDepsModule(client=client, dynamic_reads={ROUTE: config})
        ctx = ExecutionContext(deps=DepsRegistry.from_modules(module).freeze().resolve())

        port = ctx.dynamic_read.query(DynamicReadSpec(name=ROUTE))

        assert isinstance(port, DuckDbDynamicReadAdapter)
        assert port.client is client
        assert port.config is config
        assert port.statement_timeout == timedelta(seconds=3)
        assert port.tenant_placeholder_pattern == r"\$tenant\b"


# ....................... #


class TestConfig:
    def test_provenance_and_namespace_have_no_default(self) -> None:
        with pytest.raises(TypeError):
            DuckDbDynamicReadConfig(namespace="gold")  # type: ignore[call-arg]

        with pytest.raises(TypeError):
            DuckDbDynamicReadConfig(provenance="trusted")  # type: ignore[call-arg]

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_a_non_positive_statement_timeout_is_refused(self, timeout: timedelta) -> None:
        with pytest.raises(CoreException) as ei:
            _config(statement_timeout=timeout)

        assert ei.value.code == "dynamic_read_timeout_invalid"

    def test_an_empty_namespace_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _config(namespace=" ")

        assert ei.value.code == "duckdb_dynamic_read_namespace_invalid"

    def test_the_adapter_refuses_a_timeout_its_config_does_not_carry(self) -> None:
        with pytest.raises(CoreException) as ei:
            DuckDbDynamicReadAdapter(
                client=_routed(),
                spec=DynamicReadSpec(name=ROUTE),
                config=_config(),
                statement_timeout=timedelta(seconds=1),
            )

        assert ei.value.kind is ExceptionKind.INTERNAL


# ....................... #

_PERMISSION = (PERMISSION_DENIED_CODE, ExceptionKind.PRECONDITION)
_INVALID = (STATEMENT_INVALID_CODE, ExceptionKind.VALIDATION)
_INFRA = ("core.infrastructure", ExceptionKind.INFRASTRUCTURE)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (duckdb.InterruptException("Interrupted!"), (TIMEOUT_CODE, ExceptionKind.TIMEOUT)),
        (duckdb.PermissionException("File system LocalFileSystem has been disabled"), _PERMISSION),
        (duckdb.HTTPException("HTTP GET error reading 'x' (HTTP 403 Forbidden)"), _PERMISSION),
        (duckdb.HTTPException("Unable to connect to URL x: Forbidden (HTTP code 403)"),
         _PERMISSION),
        (duckdb.HTTPException("HTTP GET error reading 'x' (HTTP 401 Unauthorized)"), _PERMISSION),
        (duckdb.HTTPException("HTTP GET error reading 'x' (HTTP 500)"), _INFRA),
        (duckdb.Error("returned a non-200 status code (NotFound_404)"), _PERMISSION),
        (duckdb.Error("OAuth2 token request failed (Unauthorized_401)"), _PERMISSION),
        (duckdb.Error("returned a non-200 status code (Forbidden_403)"), _PERMISSION),
        (duckdb.Error("No region was provided via the vended credentials"), _PERMISSION),
        (duckdb.Error("something else entirely"), _INFRA),
        (duckdb.InvalidInputException(
            'Cannot execute statement of type "INSERT" on database "lake" which is attached in '
            "read-only mode!"
        ), (WRITE_REFUSED_CODE, ExceptionKind.PRECONDITION)),
        (duckdb.InvalidInputException(
            'Cannot change configuration option "x" - the configuration has been locked'
        ), _PERMISSION),
        (duckdb.InvalidInputException("Displaying unredacted secrets is disabled"), _PERMISSION),
        (duckdb.InvalidInputException("Values were not provided for parameters: x"), _INVALID),
        (duckdb.ParserException("syntax error at or near SELEC"), _INVALID),
        (duckdb.BinderException('Referenced column "nope" not found'), _INVALID),
        (duckdb.CatalogException('schema "gold" does not exist'), _INVALID),
        (duckdb.IOException("Timeout was reached"), _INFRA),
        (duckdb.OutOfMemoryException("could not allocate"), _INFRA),
    ],
    ids=lambda value: type(value).__name__ if isinstance(value, Exception) else "",
)
def test_each_engine_failure_maps_as_section_4_5_says(
    error: Exception, expected: tuple[str, ExceptionKind]
) -> None:
    mapped = dynamic_read_error(error, route=ROUTE)

    assert isinstance(mapped, CoreException)
    assert (mapped.code, mapped.kind) == expected
    assert (mapped.details or {}).get("route") == ROUTE


def test_a_refusal_carries_the_engine_message_for_diagnosis() -> None:
    message = "No region was provided via the vended credentials"

    mapped = dynamic_read_error(duckdb.Error(message), route=ROUTE)

    assert isinstance(mapped, CoreException)
    assert mapped.details == {"route": ROUTE, "detail": message}


def test_a_conversion_failure_does_not_echo_the_value() -> None:
    error = duckdb.ConversionException("Could not convert string 'secret-row' to INT32")

    mapped = dynamic_read_error(error, route=ROUTE)

    assert isinstance(mapped, CoreException)
    assert mapped.code == STATEMENT_INVALID_CODE
    assert "secret-row" not in repr(mapped.details)


@pytest.mark.parametrize(
    ("raised", "code", "details"),
    [
        (
            exc.validation("x", code=MULTI_STATEMENT_CODE, details={"statement_count": 2}),
            MULTI_STATEMENT_CODE,
            {"route": ROUTE},
        ),
        (
            exc.precondition("x", code=WRITE_REFUSED_CODE, details={"statement_type": "COPY"}),
            WRITE_REFUSED_CODE,
            {"route": ROUTE, "statement_type": "COPY"},
        ),
        (exc.infrastructure("x", code=QUERY_TIMEOUT_CODE), TIMEOUT_CODE, {"route": ROUTE}),
        (
            exc.validation("x", code=STATEMENT_INVALID_CODE, details={"detail": "dup: id"}),
            STATEMENT_INVALID_CODE,
            {"route": ROUTE, "detail": "dup: id"},
        ),
    ],
)
def test_the_client_refusals_gain_the_route(
    raised: CoreException, code: str, details: dict[str, Any]
) -> None:
    mapped = dynamic_read_error(raised, route=ROUTE)

    assert isinstance(mapped, CoreException)
    assert (mapped.code, mapped.details) == (code, details)


def test_anything_else_passes_through_unchanged() -> None:
    attestation = exc.configuration("x", code="duckdb_session_attestation_failed")

    for error in (attestation, KeyError(uuid4())):
        assert dynamic_read_error(error, route=ROUTE) is error
