"""The self-hosted durable→registry bridge.

# covers: forze_kits.integrations.durable.operation_bridge

``DurableFunctionSpec.operation`` had auto-bridging only on the Inngest tier; the
self-hosted twin validates the run's stored JSON into the spec's args type and
dispatches through ``run_operation`` — plans and hooks apply, no bypass — so every
self-hosted app stops hand-writing the same body.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.operations import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableServiceIdentity,
    operation_durable_handler,
    register_operation_functions,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Args(BaseModel):
    order_id: str


class _Out(BaseModel):
    handled: str


async def _handle(args: _Args) -> _Out:
    return _Out(handled=args.order_id)


def _operations():  # type: ignore[no-untyped-def]
    return OperationRegistry().set_handler("sync.run", lambda _ctx: _handle).freeze()


def _spec(operation: str | None = "sync.run") -> DurableFunctionSpec[_Args, _Out]:
    return DurableFunctionSpec(
        name="sync",
        run=DurableFunctionInvokeSpec(args_type=_Args),
        operation=operation,
        triggers=(DurableFunctionCronTrigger(expression="0 * * * *"),),
    )


# ....................... #


@pytest.mark.asyncio
async def test_bridge_validates_input_and_dispatches_the_operation() -> None:
    handler = operation_durable_handler(_spec(), _operations())
    ctx = context_from_modules(MockDepsModule())

    output = await handler(ctx, {"order_id": "o-1"})

    assert output == {"handled": "o-1"}  # BaseModel output lands as JSON


@pytest.mark.asyncio
async def test_malformed_stored_input_is_a_clean_precondition() -> None:
    handler = operation_durable_handler(_spec(), _operations())
    ctx = context_from_modules(MockDepsModule())

    with pytest.raises(CoreException) as caught:
        await handler(ctx, {"order_id": 123456})  # wrong type in the stored record

    assert caught.value.code == "durable_input_invalid"


@pytest.mark.asyncio
async def test_none_output_stays_none() -> None:
    async def _raw(args: _Args) -> None:
        return None

    operations = OperationRegistry().set_handler("sync.run", lambda _ctx: _raw).freeze()
    handler = operation_durable_handler(_spec(), operations)
    ctx = context_from_modules(MockDepsModule())

    assert await handler(ctx, {"order_id": "o-1"}) is None


@pytest.mark.asyncio
async def test_dict_output_is_json_encoded_at_the_bridge() -> None:
    # A dict output gets the same encoding a BaseModel gets from
    # model_dump(mode="json") — live UUID/datetime leaves must not ride into the
    # JSON-only run store and fail there, after the operation already ran.
    from datetime import UTC, datetime
    from uuid import UUID

    async def _raw(args: _Args) -> dict[str, object]:
        return {
            "id": UUID("11111111-1111-1111-1111-111111111111"),
            "at": datetime(2026, 7, 24, tzinfo=UTC),
        }

    operations = OperationRegistry().set_handler("sync.run", lambda _ctx: _raw).freeze()
    handler = operation_durable_handler(_spec(), operations)
    ctx = context_from_modules(MockDepsModule())

    output = await handler(ctx, {"order_id": "o-1"})

    assert output == {
        "id": "11111111-1111-1111-1111-111111111111",
        "at": "2026-07-24T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_non_json_output_is_refused_at_the_bridge() -> None:
    # The old fall-through returned ANY non-BaseModel value unchanged, deferring
    # the failure to durable result persistence. It must fail here, typed.
    async def _raw(args: _Args) -> object:
        return object()

    operations = OperationRegistry().set_handler("sync.run", lambda _ctx: _raw).freeze()
    handler = operation_durable_handler(_spec(), operations)
    ctx = context_from_modules(MockDepsModule())

    with pytest.raises(CoreException) as caught:
        await handler(ctx, {"order_id": "o-1"})

    assert caught.value.code == "durable_output_invalid"


@pytest.mark.asyncio
async def test_unencodable_dict_output_is_refused_at_the_bridge() -> None:
    async def _raw(args: _Args) -> dict[str, object]:
        return {"payload": object()}  # a dict, but not JSON-encodable

    operations = OperationRegistry().set_handler("sync.run", lambda _ctx: _raw).freeze()
    handler = operation_durable_handler(_spec(), operations)
    ctx = context_from_modules(MockDepsModule())

    with pytest.raises(CoreException) as caught:
        await handler(ctx, {"order_id": "o-1"})

    assert caught.value.code == "durable_output_invalid"


def test_spec_without_operation_is_refused() -> None:
    with pytest.raises(CoreException, match="declares no operation"):
        operation_durable_handler(_spec(operation=None), _operations())


@pytest.mark.asyncio
async def test_register_operation_functions_registers_each_body() -> None:
    registry = DurableFunctionRegistry()
    register_operation_functions(registry, [_spec()], operations=_operations())

    assert registry.registered("sync")

    ctx = context_from_modules(MockDepsModule())
    output = await registry.get("sync")(ctx, {"order_id": "o-2"})
    assert output == {"handled": "o-2"}



# ....................... #

_SERVICE = AuthnIdentity(principal_id=UUID("00000000-0000-0000-0000-00000000005e"))
_CALLER = AuthnIdentity(principal_id=UUID("00000000-0000-0000-0000-0000000000ca"))
_TENANT = TenantIdentity(tenant_id=UUID("00000000-0000-0000-0000-00000000000a"))


def _guarded(seen: list[Any]) -> Any:
    """An ``AuthnRequired`` operation recording the principal and tenant it ran as."""

    def factory(ctx: Any) -> Any:
        async def _run(args: _Args) -> _Out:
            seen.append((ctx.inv_ctx.get_authn(), ctx.inv_ctx.get_tenant()))
            return _Out(handled=args.order_id)

        return _run

    return (
        OperationRegistry(handlers={"sync.run": factory})
        .patch(str_key_selector.exact("sync.run"))
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
        .freeze()
    )


def test_a_service_identity_must_be_acknowledged() -> None:
    with pytest.raises(CoreException) as caught:
        DurableServiceIdentity(authn=_SERVICE)

    assert caught.value.code == "durable_unverified_identity_unacknowledged"


@pytest.mark.asyncio
async def test_without_an_identity_a_guarded_operation_refuses_even_under_a_caller() -> None:
    seen: list[Any] = []
    handler = operation_durable_handler(_spec(), _guarded(seen))
    ctx = context_from_modules(MockDepsModule())

    # The enqueuing request's principal must not leak into the run.
    with ctx.inv_ctx.bind_identity(authn=_CALLER):
        with pytest.raises(CoreException) as caught:
            await handler(ctx, {"order_id": "o-1"})

    assert caught.value.code == "auth_required"
    assert seen == []


@pytest.mark.asyncio
async def test_a_service_identity_runs_the_guarded_operation_as_itself() -> None:
    seen: list[Any] = []
    identity = DurableServiceIdentity(authn=_SERVICE, acknowledge_unverified_identity=True)
    registry = DurableFunctionRegistry()
    register_operation_functions(
        registry, [_spec()], operations=_guarded(seen), identity=identity
    )
    ctx = context_from_modules(MockDepsModule())

    with ctx.inv_ctx.bind_identity(authn=_CALLER, tenant=_TENANT):
        output = await registry.get("sync")(ctx, {"order_id": "o-3"})

    assert output == {"handled": "o-3"}
    # The service principal, never the caller; the tenant stays the run's.
    assert seen == [(_SERVICE, _TENANT)]


@pytest.mark.asyncio
async def test_the_runner_binds_no_principal_for_an_untagged_run() -> None:
    """``run_now`` inside a request runs the body as no one, as recovery would."""

    seen: list[Any] = []

    async def body(ctx: Any, _input: Any) -> None:
        seen.append((ctx.inv_ctx.get_authn(), ctx.inv_ctx.get_tenant()))

    registry = DurableFunctionRegistry()
    registry.register("plain", body)
    ctx = context_from_modules(MockDepsModule())

    # No tenant bound, so the run is untagged: a tagged run's tenant binding already
    # cleared the principal, and this is the path that used to inherit it.
    with ctx.inv_ctx.bind_identity(authn=_CALLER):
        await DurableFunctionRunner(registry=registry).run_now(ctx, "plain")

    assert seen == [(None, None)]
