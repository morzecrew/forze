"""Registry-backed durable-function bodies for the self-hosted tier.

:attr:`~forze.application.contracts.durable.function.DurableFunctionSpec.operation`
declares "this durable function IS this registry operation" — and the Inngest tier
auto-bridges it (``InngestFunctionBinding.for_registry_operation``). The self-hosted
tier had no twin: every app hand-wrote the same body that validates the stored JSON
into the spec's args type and dispatches through ``run_operation``. This module is
that twin — one call registers every operation-backed spec on the
:class:`~forze_kits.integrations.durable.registry.DurableFunctionRegistry` the
runner and recovery scanner read.

Identity note: a durable run executes under whatever the runner's scope binds —
there is **no principal**. An operation guarded by
:class:`~forze.application.hooks.authn.AuthnRequired` will therefore refuse a
scheduled or recovered run; see the guard's documentation before pointing a spec at
a guarded operation.
"""

from typing import Any

from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError, to_jsonable_python

from forze.application.contracts.durable.function import DurableFunctionSpec
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations import FrozenOperationRegistry
from forze.application.execution.operations.run.invoke import run_durable_function
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .registry import DurableFunctionHandler, DurableFunctionRegistry

# ----------------------- #

__all__ = [
    "operation_durable_handler",
    "register_operation_functions",
]


def operation_durable_handler(
    spec: DurableFunctionSpec[Any, Any],
    operations: FrozenOperationRegistry,
) -> DurableFunctionHandler:
    """Build the durable body for a spec whose ``operation`` names a registry op.

    The body validates the run's stored JSON input into the spec's args type (a
    malformed record is a clean precondition, not a raw ``ValidationError`` deep in
    the runner), dispatches through ``run_operation`` — plans and hooks apply, no
    bypass — and returns the output as JSON (durable step/results storage is
    JSON-only by contract). An output that is not a BaseModel, a JSON-encodable
    dict, or ``None`` is refused here (``durable_output_invalid``) rather than
    during result persistence, where the operation's work would already be
    committed.
    """

    if spec.operation is None:
        raise exc.configuration(
            f"Durable function {spec.name!r} declares no operation; register a "
            "hand-written body instead, or set DurableFunctionSpec.operation.",
        )

    args_type = spec.run.args_type

    async def handler(ctx: ExecutionContext, input_json: JsonDict | None) -> JsonDict | None:
        try:
            args = args_type.model_validate(input_json or {})

        except ValidationError as error:
            # Deterministic: replaying the same stored input can never converge.
            raise exc.precondition(
                f"Durable run input for {spec.name!r} does not validate as {args_type.__name__}.",
                code="durable_input_invalid",
            ) from error

        output = await run_durable_function(spec, operations, ctx, args)

        if output is None:
            return None

        if isinstance(output, BaseModel):
            return output.model_dump(mode="json")

        # The run store is JSON-only, so anything else must be a JSON dict — and it
        # must fail HERE, at the bridge, not later inside result persistence when
        # the operation's work is already done and the failure reads as a store bug.
        if not isinstance(output, dict):
            raise exc.configuration(
                f"Durable function {spec.name!r} produced a "
                f"{type(output).__name__}; a registry-backed durable body must "
                "return a BaseModel, a JSON dict, or None.",
                code="durable_output_invalid",
            )

        try:
            # The same encoding a BaseModel gets from model_dump(mode="json"): live
            # UUID/datetime/Decimal leaves become JSON scalars instead of riding
            # through and failing in the run store.
            return to_jsonable_python(output)

        except PydanticSerializationError as error:
            raise exc.configuration(
                f"Durable function {spec.name!r} produced a dict that does not "
                "JSON-encode; durable results storage is JSON-only.",
                code="durable_output_invalid",
            ) from error

    return handler


# ....................... #


def register_operation_functions(
    registry: DurableFunctionRegistry,
    specs: list[DurableFunctionSpec[Any, Any]] | tuple[DurableFunctionSpec[Any, Any], ...],
    *,
    operations: FrozenOperationRegistry,
) -> None:
    """Register every operation-backed spec in *specs* on *registry*.

    Specs without an ``operation`` are refused rather than skipped: a silent skip
    would leave their runs unresumable (no registered body) and read as "wired".
    """

    for spec in specs:
        registry.register(str(spec.name), operation_durable_handler(spec, operations))
