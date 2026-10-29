"""Registry-backed durable-function bodies for the self-hosted tier.

:attr:`~forze.application.contracts.durable.function.DurableFunctionSpec.operation`
declares "this durable function IS this registry operation" — and the Inngest tier
auto-bridges it (``InngestFunctionBinding.for_registry_operation``). The self-hosted
tier had no twin: every app hand-wrote the same body that validates the stored JSON
into the spec's args type and dispatches through ``run_operation``. This module is
that twin — one call registers every operation-backed spec on the
:class:`~forze_kits.integrations.durable.registry.DurableFunctionRegistry` the
runner and recovery scanner read.

Identity note: a durable run executes with **no principal** — the runner binds none,
whether the run started under a request or was recovered — so an operation guarded by
:class:`~forze.application.hooks.authn.AuthnRequired` refuses it. Name who runs it with
:class:`DurableServiceIdentity`, backed by an API key verified on every run or asserted by
the wiring alone; the run's tenant still comes from the run.
"""

import asyncio
import contextvars
from typing import Any, final

import attrs
from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError, to_jsonable_python

from forze.application.contracts.authn import (
    AuthnIdentity,
    AuthnSpec,
    bind_authn_admission,
    current_authn_admission,
)
from forze.application.contracts.durable.function import (
    DurableFunctionSpec,
    current_durable_run,
)
from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.contracts.tenancy import TenantIdentity, parse_tenant_hint
from forze.application.execution.context import ExecutionContext
from forze.application.execution.ingress import (
    authenticate_api_key,
    resolve_principal_tenant,
)
from forze.application.execution.operations import FrozenOperationRegistry
from forze.application.execution.operations.run.invoke import run_durable_function
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict

from .._logger import logger
from .registry import DurableFunctionHandler, DurableFunctionRegistry

# ----------------------- #

__all__ = [
    "DurableServiceIdentity",
    "operation_durable_handler",
    "register_operation_functions",
]


@final
@attrs.frozen(kw_only=True)
class DurableServiceIdentity:
    """Run an operation-backed durable body as a fixed service principal.

    A scheduled or recovered run presents no credential of its own. With *api_key*, the
    bridge resolves that secret and verifies it through the authn port *authn_spec*
    selects before every run, as the HTTP edge verifies a key, and the body runs as the
    identity the verifier returns, with its current roles and claims: revoking the key,
    letting it expire or disabling the principal fails the next run (``authentication``,
    never retried). Without one, whoever it runs as is the application's word: authn and
    authz decide for it and audit records it as the actor, but nothing is checked, expired
    or revoked. Either way the run's tenant comes from the run, never from here.

    :param authn: The principal the body runs as. With *api_key*, the key must
        authenticate this principal, as itself and not on behalf of another, or the run is
        refused.
    :param api_key: Where the key is read from at run time; the key itself is never held
        here, nor written to a run or schedule row.
    :param authn_spec: The authn route *api_key* verifies against; required with it.
    :param tenancy_route: The tenancy route that checks the principal belongs to a tagged
        run's tenant; defaults to *authn_spec*'s name, as over MCP and ``forze ops``.
    :param acknowledge_unverified_identity: Must be ``True`` when there is no *api_key*;
        anything else raises ``configuration``, so the choice is never made by accident.
    """

    authn: AuthnIdentity
    api_key: SecretRef | None = None
    authn_spec: AuthnSpec | None = None
    tenancy_route: str | None = None
    acknowledge_unverified_identity: bool = False

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (self.api_key is None) != (self.authn_spec is None):
            raise exc.configuration(
                "DurableServiceIdentity verifies an API key against an authn route: pass "
                "api_key and authn_spec together",
                code="durable_service_identity_incomplete",
            )

        if self.api_key is None and self.acknowledge_unverified_identity is not True:
            raise exc.configuration(
                "DurableServiceIdentity binds a principal nothing verifies: pass "
                "acknowledge_unverified_identity=True to accept that, or an api_key",
                code="durable_unverified_identity_unacknowledged",
            )


# ....................... #


async def _admit(
    identity: DurableServiceIdentity | None,
    ctx: ExecutionContext,
    tenant: TenantIdentity | None,
) -> tuple[AuthnIdentity | None, object | None]:
    """The principal one run executes as under *tenant*, and the admission its body carries.

    Only a verified key leaves an admission (what the identity plane's verifier records, so
    an issue inside the body re-checks the service key). It is verified in a copy of the
    context whose admission starts empty, so nothing the verifier records reaches the
    caller's task, or survives the body.
    """

    if identity is None:
        return None, None

    if identity.api_key is None or identity.authn_spec is None:
        return identity.authn, None

    with bind_authn_admission(None):
        scope = contextvars.copy_context()

    verify = _verified(identity, identity.api_key, identity.authn_spec, ctx, tenant)
    authn = await asyncio.create_task(verify, context=scope)

    return authn, scope.run(current_authn_admission)


# ....................... #


async def _verified(
    identity: DurableServiceIdentity,
    ref: SecretRef,
    spec: AuthnSpec,
    ctx: ExecutionContext,
    tenant: TenantIdentity | None,
) -> AuthnIdentity:
    """Verify *identity*'s key for one run under *tenant*; log a failure without the key.

    :raises CoreException: ``authentication`` for an unknown, revoked or expired key, a key
        of another principal (``durable_service_identity_mismatch``), one acting for another
        (``durable_service_identity_delegated``), or one whose principal or own tenant is not
        the run's (``tenant_mismatch``, ``tenant_conflict``); ``internal``
        (``durable_service_identity_unchecked``) for an error that is not a
        ``CoreException``; the kind and code of whatever else the secrets backend or authn
        raises, under a message of its own; whatever tenancy raises.
    """

    try:
        try:
            raw = await ctx.deps.provide(SecretsDepKey).resolve_str(ref)
            result = await authenticate_api_key(ctx, spec, raw)

        except CoreException as error:
            # Kind and code carry the reason and the retry decision; the message is the
            # backend's, may quote the key, and the run row stores it: rebuilt, not kept.
            raise CoreException(
                error.kind,
                f"The durable service identity did not verify ({error.kind.value}, {error.code})",
                code=error.code,
            ) from None

        except Exception as error:
            # Its message may quote the key, and the run row stores the message: type only.
            raise exc.internal(
                f"The durable service identity could not be checked ({type(error).__name__})",
                code="durable_service_identity_unchecked",
            ) from None

        if result.identity.principal_id != identity.authn.principal_id:
            raise exc.authentication(
                "The durable service identity's API key authenticates another principal",
                code="durable_service_identity_mismatch",
            )

        if result.identity.actor is not None:
            raise exc.authentication(
                "The durable service identity's API key acts on behalf of another principal",
                code="durable_service_identity_delegated",
            )

        if tenant is not None:
            # As at the HTTP edge: refused unless the principal belongs to the run's tenant
            # (or, with no tenancy resolver, the key was issued for it).
            await resolve_principal_tenant(
                ctx,
                result,
                route=identity.tenancy_route or str(spec.name),
                requested=tenant.tenant_id,
            )

        elif parse_tenant_hint(result.issuer_tenant_hint) is not None:
            raise exc.authentication(
                "The durable service identity's API key belongs to a tenant; the run to none",
                code="tenant_conflict",
            )

    except CoreException as error:
        run = current_durable_run()
        verdict = (
            "its service identity did not verify"
            if error.kind is ExceptionKind.AUTHENTICATION
            else "its service identity could not be checked, which says nothing of the key"
        )
        # Kind and code only: nothing about the key reaches the log.
        logger.warning(
            "Durable run %s (%s) failed: %s (%s/%s)",
            run.run_id if run is not None else "-",
            run.name if run is not None else "-",
            verdict,
            error.kind.value,
            error.code,
        )

        raise

    return result.identity


# ....................... #


def operation_durable_handler(
    spec: DurableFunctionSpec[Any, Any],
    operations: FrozenOperationRegistry,
    *,
    identity: DurableServiceIdentity | None = None,
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

    *identity* is the principal the operation runs as, its key verified on each call;
    ``None`` runs it as no one, so a guarded operation refuses the run. The ambient
    principal is never inherited.
    """

    if spec.operation is None:
        raise exc.configuration(
            f"Durable function {spec.name!r} declares no operation; register a "
            "hand-written body instead, or set DurableFunctionSpec.operation.",
        )

    args_type = spec.run.args_type

    async def handler(ctx: ExecutionContext, input_json: JsonDict | None) -> JsonDict | None:
        try:
            # ``is None``, not falsy: a root-model input may be stored as ``0`` or ``""``.
            args = args_type.model_validate({} if input_json is None else input_json)

        except ValidationError as error:
            # Deterministic: replaying the same stored input can never converge.
            raise exc.precondition(
                f"Durable run input for {spec.name!r} does not validate as {args_type.__name__}.",
                code="durable_input_invalid",
            ) from error

        # Exactly the declared principal, or none: never whoever enqueued the run.
        tenant = ctx.inv_ctx.get_tenant()
        authn, admission = await _admit(identity, ctx, tenant)

        with (
            bind_authn_admission(admission),
            ctx.inv_ctx.bind_identity(authn=authn, tenant=tenant),
        ):
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
    identity: DurableServiceIdentity | None = None,
) -> None:
    """Register every operation-backed spec in *specs* on *registry*.

    Specs without an ``operation`` are refused rather than skipped: a silent skip
    would leave their runs unresumable (no registered body) and read as "wired".
    *identity* is passed to :func:`operation_durable_handler` for each.
    """

    for spec in specs:
        registry.register(
            str(spec.name),
            operation_durable_handler(spec, operations, identity=identity),
        )
