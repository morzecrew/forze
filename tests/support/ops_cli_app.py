"""A sample ``forze ops`` application for the CLI tests, loadable by import string.

Operations probe what a call binds (identity, tenant, metadata) and fail on demand, so the
exit-code table and the envelope can be driven end to end. Every declaration builds a
``SERVERLESS`` runtime per call over one shared :class:`MockState`, so a write in one call
is visible to the next, and API keys verify through the mock identity plane's real authn
orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from datetime import date
from enum import Enum, IntEnum, StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, SecretStr

from forze.application.contracts.authn import AuthnIdentity, AuthnSpec
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import (
    DeploymentProfile,
    ExecutionContext,
    ExecutionRuntime,
    build_runtime,
)
from forze.application.execution.context.commit_state import mark_commit_started
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.application.hooks.tenancy import TenantRequired
from forze.base.exceptions import CoreException, DenialPosture, ExceptionKind, exc
from forze.domain.models import BaseDTO
from forze_cli.ops import (
    AnonymousIdentity,
    ApiKeyIdentity,
    OpsApp,
    StaticIdentity,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.execution.module import MockIdentityConfig

# ----------------------- #

API_KEY = "ops-key"
"""Authenticates :data:`OPERATOR`, who belongs to no tenant."""

MEMBER_KEY = "member-key"
"""Authenticates :data:`MEMBER`, a member of :data:`TENANT`."""

ISSUER_KEY = "issuer-key"
"""Authenticates :data:`OPERATOR` with :data:`TENANT` carried on the credential."""

OPERATOR = UUID("00000000-0000-4000-8000-00000000000a")
MEMBER = UUID("00000000-0000-4000-8000-00000000000b")
TENANT = UUID("00000000-0000-4000-8000-0000000000c1")

AUTHN = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))

STATE = MockState()

ADDED: list[str] = []
"""What ``notes.add`` handled, so a test can tell a replay from a re-run."""

SHUTDOWNS: list[str] = []
"""One entry per runtime shutdown."""

# An application may well log to stdout; the CLI must keep its document clean anyway.
_chatty_logger = logging.getLogger("ops_cli_app")
_chatty_logger.addHandler(logging.StreamHandler(sys.stdout))
_chatty_logger.propagate = False

# ....................... #


class Bound(BaseDTO):
    principal_id: UUID | None
    tenant_id: UUID | None
    execution_id: UUID
    correlation_id: UUID
    causation_id: UUID | None
    idempotency_key: str | None
    deadline_set: bool


class Text(BaseDTO):
    text: str


class Count(BaseDTO):
    count: int


class Fail(BaseDTO):
    kind: ExceptionKind | None = None
    """No kind succeeds, so a batch can fail on one line of several."""


class Slow(BaseDTO):
    seconds: float
    after_commit: bool = False


class Secret(BaseDTO):
    token: SecretStr


class Deny(BaseDTO):
    resource_type: str


class Mode(StrEnum):
    PLAIN = "plain"
    LOUD = "loud"


class Rank(IntEnum):
    """Pydantic reads an ``IntEnum`` from a string, as it reads an ``int``."""

    FIRST = 1
    SECOND = 2


class Code(Enum):
    """Pydantic matches a plain enum's value exactly: ``"1"`` is not ``1``."""

    ONE = 1


class Flags(BaseDTO):
    """Flat scalars only: ``forze ops call`` takes typed flags for it."""

    text: str
    count: int = 1
    ratio: float | None = None
    loud: bool = False
    day: date | None = None
    ref: UUID | None = None
    mode: Mode = Mode.PLAIN
    level: Literal["low", "high"] = "low"
    retry_after: Annotated[int, Field(ge=0)] | None = None
    rank: Rank = Rank.FIRST


class Nested(BaseDTO):
    tags: list[str]


class Collides(BaseDTO):
    """A field spelled like a ``call`` option: no typed flags."""

    timeout: float = 1.0


class Coded(BaseDTO):
    code: Code = Code.ONE


class Aliased(BaseDTO):
    """Read by alias only; a typed flag spells the field's own name."""

    display_name: str = Field(alias="displayName")


# ....................... #


def _bound(ctx: ExecutionContext) -> Callable[[None], Awaitable[Bound]]:
    async def handler(_args: None) -> Bound:
        inv = ctx.inv_ctx
        authn, tenant, meta = inv.get_authn(), inv.get_tenant(), inv.get_metadata()

        if meta is None:
            raise exc.internal("no metadata bound")

        return Bound(
            principal_id=authn.principal_id if authn is not None else None,
            tenant_id=tenant.tenant_id if tenant is not None else None,
            execution_id=meta.execution_id,
            correlation_id=meta.correlation_id,
            causation_id=meta.causation_id,
            idempotency_key=inv.get_idempotency_key(),
            deadline_set=inv.get_deadline() is not None,
        )

    return handler


async def _echo(args: Text) -> Text:
    return args


async def _returned(args: Any) -> Any:
    return args


async def _fail(args: Fail) -> None:
    if args.kind is not None:
        raise CoreException(args.kind, f"failed as {args.kind}", code=f"probe_{args.kind}")


async def _deny(args: Deny) -> None:
    raise exc.authorization("denied", resource_type=args.resource_type)


async def _add(args: Text) -> Count:
    ADDED.append(args.text)

    return Count(count=len(ADDED))


async def _slow(args: Slow) -> None:
    # Stands in for a root transaction that committed before the cancel lands.
    if args.after_commit:
        mark_commit_started()

    sys.stderr.write("ops-sample: slow started\n")
    sys.stderr.flush()
    await asyncio.sleep(args.seconds)


async def _chatty(_args: None) -> Text:
    print("printed during the call")
    _chatty_logger.warning("logged during the call")
    sys.stdout.write("written during the call\n")

    return Text(text="clean")


async def _secret(args: Secret) -> Text:
    return Text(text=f"{len(args.token.get_secret_value())} characters")


async def _ok(_args: None) -> None:
    return None


async def _cancelled(_args: None) -> None:
    # A task the operation awaited was cancelled by someone other than the CLI.
    raise asyncio.CancelledError


# ....................... #


def _descriptor(input_type: Any = None, output_type: Any = None, **kw: Any) -> OperationDescriptor:
    return OperationDescriptor(input_type=input_type, output_type=output_type, **kw)


def build_registry(*, sensitive: bool = False) -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "probe.bound": _bound,
            "probe.echo": lambda _c: _echo,
            "probe.secure": lambda _c: _ok,
            "probe.tenanted": lambda _c: _ok,
            "probe.fail": lambda _c: _fail,
            "probe.fail_write": lambda _c: _fail,
            "probe.fail_keyed": lambda _c: _fail,
            "probe.slow": lambda _c: _slow,
            "probe.chatty": lambda _c: _chatty,
            "probe.secret": lambda _c: _secret,
            "probe.deny": lambda _c: _deny,
            "probe.cancelled": lambda _c: _cancelled,
            "probe.flags": lambda _c: _returned,
            "probe.nested": lambda _c: _ok,
            "probe.collides": lambda _c: _returned,
            "probe.aliased": lambda _c: _returned,
            "probe.coded": lambda _c: _returned,
            "notes.add": lambda _c: _add,
        },
        descriptors={
            "probe.bound": _descriptor(output_type=Bound, title="What a call binds"),
            "probe.echo": _descriptor(Text, Text, description="Echo the text.\nSecond line."),
            "probe.secure": _descriptor(description="Needs a principal."),
            "probe.tenanted": _descriptor(description="Needs a tenant."),
            "probe.fail": _descriptor(Fail, sensitive=sensitive),
            "probe.fail_write": _descriptor(Fail),
            "probe.fail_keyed": _descriptor(Fail),
            "probe.slow": _descriptor(Slow),
            "probe.chatty": _descriptor(output_type=Text),
            "probe.secret": _descriptor(Secret, Text),
            "probe.deny": _descriptor(Deny),
            "probe.cancelled": _descriptor(),
            "probe.flags": _descriptor(Flags, Flags),
            "probe.nested": _descriptor(Nested),
            "probe.collides": _descriptor(Collides, Collides),
            "probe.aliased": _descriptor(Aliased),
            "probe.coded": _descriptor(Coded),
            "notes.add": _descriptor(Text, Count, title="Add a note"),
        },
    )

    for op in (
        "probe.bound",
        "probe.echo",
        "probe.fail",
        "probe.chatty",
        "probe.secret",
        "probe.deny",
        "probe.cancelled",
        "probe.flags",
        "probe.nested",
        "probe.collides",
        "probe.aliased",
        "probe.coded",
    ):
        reg = reg.bind(op).as_query().finish()

    reg = (
        reg.bind("probe.secure")
        .as_query()
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )
    reg = (
        reg.bind("probe.tenanted")
        .as_query()
        .bind_outer()
        .before(TenantRequired().to_step(step_id="tenant.required"))
        .finish(deep=True)
    )
    reg = (
        reg.bind("probe.fail_keyed")
        .bind_outer()
        .wrap(
            IdempotencyWrap(
                op="probe.fail_keyed", spec=IdempotencySpec(name="notes"), result_type=Count
            ).to_step()
        )
        .finish(deep=True)
    )
    reg = (
        reg.bind("notes.add")
        .bind_outer()
        .wrap(
            IdempotencyWrap(
                op="notes.add", spec=IdempotencySpec(name="notes"), result_type=Count
            ).to_step()
        )
        .finish(deep=True)
    )

    return reg.freeze()


# ....................... #


def seed() -> None:
    """Reset the shared state to the seeded identities and memberships."""

    STATE.identity.clear()
    STATE.identity["authn"] = {
        "main": {
            "api_keys": {
                API_KEY: {"subject": "operator"},
                MEMBER_KEY: {"subject": "member"},
                ISSUER_KEY: {"subject": "operator", "issuer_tenant_hint": str(TENANT)},
            },
            "principal_map": {"operator": str(OPERATOR), "member": str(MEMBER)},
        }
    }
    STATE.identity["tenants"] = {"main": {str(TENANT): {"principals": [MEMBER], "active": True}}}
    ADDED.clear()
    SHUTDOWNS.clear()


async def _shutdown(_ctx: ExecutionContext) -> None:
    sys.stderr.write("ops-sample: shutdown started\n")
    sys.stderr.flush()
    # A slow shutdown, for the signal tests.
    await asyncio.sleep(float(os.environ.get("OPS_SAMPLE_SHUTDOWN_SECONDS", "0")))
    SHUTDOWNS.append("ran")
    sys.stderr.write("ops-sample: shutdown ran\n")


def runtime(
    *,
    deployment: DeploymentProfile = DeploymentProfile.SERVERLESS,
    tenancy: bool = True,
    denial_posture: DenialPosture | None = None,
) -> ExecutionRuntime:
    # Without tenancy, the resolver sits on another route: the key's route finds none.
    identity = MockIdentityConfig(tenancy_routes=frozenset({"main" if tenancy else "elsewhere"}))

    return build_runtime(
        MockDepsModule(state=STATE, identity=identity),
        lifecycle_steps=LifecycleStep(id="ops-sample", shutdown=_shutdown),
        deployment=deployment,
        denial_posture=denial_posture,
    )


REGISTRY = build_registry()

# ....................... #
# Declarations, by import string.

API = OpsApp(
    registry=REGISTRY, runtime=runtime, identity=ApiKeyIdentity(AUTHN), include_writes=True
)

API_NO_TENANCY = OpsApp(
    registry=REGISTRY,
    runtime=lambda: runtime(tenancy=False),
    identity=ApiKeyIdentity(AUTHN),
    include_writes=True,
)

ANONYMOUS = OpsApp(registry=REGISTRY, runtime=runtime, identity=AnonymousIdentity())

STATIC = OpsApp(
    registry=REGISTRY,
    runtime=runtime,
    identity=StaticIdentity(
        authn=AuthnIdentity(principal_id=OPERATOR),
        tenant=TenantIdentity(tenant_id=TENANT),
        acknowledge_unverified_identity=True,
    ),
    include_writes=True,
)

NON_DISCLOSING = OpsApp(
    registry=REGISTRY,
    runtime=lambda: runtime(
        denial_posture=DenialPosture(mode="non_disclosing", resource_types=frozenset({"notes"}))
    ),
    identity=AnonymousIdentity(),
)


def _broken_runtime() -> ExecutionRuntime:
    credential = "marker-in-a-factory-local"  # what a crash that printed locals would leak

    raise RuntimeError(f"factory bug ({len(credential)})")


BROKEN_RUNTIME = OpsApp(registry=REGISTRY, runtime=_broken_runtime, identity=AnonymousIdentity())

NOT_SERVERLESS = OpsApp(
    registry=REGISTRY,
    runtime=lambda: runtime(deployment=DeploymentProfile.SINGLE_PROCESS),
    identity=AnonymousIdentity(),
)


def lazy() -> OpsApp:
    """A zero-argument callable returning a declaration."""

    return ANONYMOUS


def chatty() -> OpsApp:
    """Prints while it builds the declaration, as an application module may on import."""

    print("building the declaration")  # noqa: T201

    return ANONYMOUS


def unbuildable() -> OpsApp:
    credential = "marker-in-a-local"  # what a crash that printed locals would leak

    raise RuntimeError(f"the declaration refuses itself ({len(credential)})")


NOT_AN_OPS_APP = object()
