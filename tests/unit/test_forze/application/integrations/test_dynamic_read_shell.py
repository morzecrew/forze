"""The shared dynamic-read governance shell, exercised through a recording stub adapter.

The battery in ``tests/support/dynamic_read_conformance`` compares the shell across engines;
this file looks at the parts that have no engine-visible effect — what the shell *decides*
before ``_fetch_rows`` is called, and what it refuses outright.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any, final
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.dynamic_read import DynamicReadSpec
from forze.application.contracts.tenancy import TenantIdentity, TenantProviderPort
from forze.application.integrations.dynamic_read import (
    DynamicReadAdapter,
    DynamicReadRequest,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import JsonDict

pytestmark = pytest.mark.asyncio

ROUTE = "widgets"


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RecordingAdapter(DynamicReadAdapter):
    """Records the governed request and answers with a scripted row count."""

    rows: int = 0
    seen: list[DynamicReadRequest] = attrs.field(factory=list)
    rows_payload: JsonDict | None = None
    """When set, every scripted row is this mapping instead of ``{"n": i}``."""

    async def _fetch_rows(self, request: DynamicReadRequest) -> Sequence[JsonDict]:
        self.seen.append(request)
        count = min(self.rows, request.row_probe)

        if self.rows_payload is not None:
            return [dict(self.rows_payload) for _ in range(count)]

        return [{"n": index} for index in range(count)]


def _tenant(tenant_id: UUID | None) -> TenantProviderPort:
    def provider() -> TenantIdentity | None:
        return None if tenant_id is None else TenantIdentity(tenant_id=tenant_id)

    return provider


def _adapter(**overrides: Any) -> RecordingAdapter:
    spec_kwargs: dict[str, Any] = overrides.pop("spec_kwargs", {})
    return RecordingAdapter(
        spec=DynamicReadSpec(name=ROUTE, **spec_kwargs),
        statement_timeout=overrides.pop("statement_timeout", timedelta(seconds=5)),
        **overrides,
    )


# ....................... #


async def test_the_probe_asks_for_one_more_row_than_the_cap() -> None:
    """The extra row is what turns a silent truncation into a refusal."""

    adapter = _adapter(spec_kwargs={"row_cap": 7})
    await adapter.run("SELECT 1")

    assert adapter.seen[-1].row_cap == 7
    assert adapter.seen[-1].row_probe == 8


async def test_a_call_timeout_clamps_down_but_never_up() -> None:
    """The route's timeout is a ceiling; a call may only ask for less."""

    adapter = _adapter(statement_timeout=timedelta(seconds=5))

    await adapter.run("SELECT 1", options={"timeout": timedelta(seconds=1)})
    assert adapter.seen[-1].timeout == timedelta(seconds=1)

    await adapter.run("SELECT 1", options={"timeout": timedelta(seconds=30)})
    assert adapter.seen[-1].timeout == timedelta(seconds=5)


@pytest.mark.parametrize(
    "options",
    [
        {"row_cap": 0},
        {"row_cap": -3},
        {"row_cap": True},
        {"timeout": timedelta(0)},
        {"timeout": timedelta(seconds=-1)},
        {"timeout": 5},
    ],
    ids=["zero", "negative", "bool", "zero-timeout", "negative-timeout", "not-a-timedelta"],
)
async def test_a_nonsensical_call_option_is_refused(options: dict[str, Any]) -> None:
    """Clamping a bad option to something sane would hide the caller's bug.

    ``True`` is in the list because ``bool`` is an ``int`` in Python: a stray flag passed as
    ``row_cap`` would otherwise be silently accepted as "one row".
    """

    with pytest.raises(CoreException) as ei:
        await _adapter().run("SELECT 1", options=options)  # type: ignore[arg-type]

    assert ei.value.code == "dynamic_read_option_invalid"
    assert ei.value.kind == ExceptionKind.VALIDATION


async def test_the_tenant_parameter_is_merged_only_when_the_statement_references_it() -> None:
    """Advisory convenience, and only where it is asked for."""

    tenant_id = uuid4()
    adapter = _adapter(tenant_aware=True, tenant_provider=_tenant(tenant_id))

    await adapter.run("SELECT * FROM t WHERE tenant_id = %(tenant)s")
    assert adapter.seen[-1].params == {"tenant": str(tenant_id)}

    await adapter.run("SELECT * FROM t")
    assert adapter.seen[-1].params == {}


async def test_a_placeholder_that_appears_only_in_a_comment_does_not_count() -> None:
    """A commented-out reference is not a reference; the merge follows the SQL, not the text."""

    adapter = _adapter(tenant_aware=True, tenant_provider=_tenant(uuid4()))

    await adapter.run("SELECT 1 -- %(tenant)s")

    assert adapter.seen[-1].params == {}


async def test_caller_params_survive_the_merge() -> None:
    """The tenant id is added to the caller's params, never in place of them."""

    tenant_id = uuid4()
    adapter = _adapter(tenant_aware=True, tenant_provider=_tenant(tenant_id))

    await adapter.run("SELECT %(a)s, %(tenant)s", {"a": 1})

    assert adapter.seen[-1].params == {"a": 1, "tenant": str(tenant_id)}


async def test_an_untenanted_route_binds_no_tenant() -> None:
    """No tenant in context means no tenant parameter — not an empty string."""

    adapter = _adapter()

    await adapter.run("SELECT %(tenant)s")

    assert adapter.seen[-1].params == {}
    assert adapter.seen[-1].tenant_id is None


async def test_an_oversized_statement_never_reaches_the_engine() -> None:
    """Refused ahead of ``_fetch_rows``, so no connection is spent on it."""

    adapter = _adapter(spec_kwargs={"max_statement_bytes": 8})

    with pytest.raises(CoreException):
        await adapter.run("SELECT * FROM a_very_long_relation_name")

    assert adapter.seen == []


async def test_the_byte_cap_counts_bytes_not_characters() -> None:
    """A multi-byte statement is measured the way the wire measures it."""

    adapter = _adapter(spec_kwargs={"max_statement_bytes": 10})

    # Ten characters, twenty bytes in UTF-8.
    with pytest.raises(CoreException) as ei:
        await adapter.run("SELECT 'ЖЖЖЖ'")

    assert ei.value.details is not None
    assert ei.value.details["size"] > 10


async def test_a_tenant_aware_route_fails_closed_before_the_engine() -> None:
    """No bound tenant is a refusal, and it happens before ``_fetch_rows``."""

    adapter = _adapter(tenant_aware=True, tenant_provider=_tenant(None))

    with pytest.raises(CoreException) as ei:
        await adapter.run("SELECT 1")

    assert ei.value.code == "tenant_required"
    assert adapter.seen == []


async def test_a_row_type_mismatch_reports_the_fields_and_not_the_row() -> None:
    """The error says which field failed, never what the row held.

    ``str(ValidationError)`` embeds ``input_value=`` — the whole offending row — and these are
    warehouse rows on a plane pointed at BI relations. An error that egresses to the caller
    and into logs must not carry them, so what ships is the failure and not the data.
    """

    from pydantic import BaseModel

    class Expected(BaseModel):
        missing_column: str

    adapter = _adapter(
        rows=1,
        rows_payload={"customer_email": "nadia@example.com", "ssn": "123-45-6789"},
    )

    with pytest.raises(CoreException) as ei:
        await adapter.select(Expected, "SELECT * FROM gold")

    assert ei.value.code == "dynamic_read_row_type_mismatch"
    assert ei.value.details is not None

    rendered = repr(ei.value.details)

    assert "nadia@example.com" not in rendered
    assert "123-45-6789" not in rendered
    # Still actionable: the field that failed and why.
    assert "missing_column" in rendered


async def test_a_backend_that_overshoots_the_probe_still_gets_refused() -> None:
    """Defence in depth: the cap check compares against ``row_cap``, not against the probe."""

    @final
    @attrs.define(slots=True, kw_only=True, frozen=True)
    class Overshooting(DynamicReadAdapter):
        async def _fetch_rows(self, request: DynamicReadRequest) -> Sequence[JsonDict]:
            _ = request
            return [{"n": index} for index in range(50)]

    adapter = Overshooting(
        spec=DynamicReadSpec(name=ROUTE, row_cap=5),
        statement_timeout=timedelta(seconds=1),
    )

    with pytest.raises(CoreException) as ei:
        await adapter.run("SELECT 1")

    assert ei.value.code == "dynamic_read_row_cap_exceeded"
