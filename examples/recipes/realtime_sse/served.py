"""The SSE recipe, served on the mock — with a control plane that can fire a signal.

`app.py` shows the transport against a hand-driven mailbox. This shows the *served* form:
two API keys, two principals, one server, and `POST /_mock/emit` firing one signal at one
audience on demand — which is what a developer building a notification badge actually needs,
and what a traffic generator cannot give them.

The seam is the point. The realtime egress plane lives above `forze_mock`, so the mock server
cannot deliver a signal itself; it hands the signal to `MockApp.on_emit`, which is the app's
own code writing into the app's own mailbox. That is the same rule as `build_app`: nothing
transport- or identity-shaped is owned by the server.

    FORZE_MOCK_SERVER=1 forze mock serve examples.recipes.realtime_sse.served:mock_app
    curl -X POST localhost:8000/_mock/emit \\
         -d '{"audience_kind":"principal","audience_name":"<id>","event":"order.shipped","payload":{}}'

Exercised by ``tests/unit/test_examples/test_realtime_sse_served.py``.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.primitives import HlcTimestamp, uuid7
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.lifespan import runtime_lifespan
from forze_fastapi.middlewares import InvocationMetadataMiddleware, SecurityContextMiddleware
from forze_fastapi.realtime import attach_realtime_sse_route
from forze_fastapi.security import AuthnRequirement, HeaderApiKeyAuthn
from forze_identity.authz import policy_principal_spec
from forze_identity.builtin.local import from_mapping, local_identity_deps
from forze_mock.seeding import SeedPlan, spec_seed
from forze_mock.server import MockApp

# ----------------------- #

ADA = UUID("11111111-1111-1111-1111-111111111111")
BOB = UUID("22222222-2222-2222-2222-222222222222")

AUTHN = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))

IDENTITY = from_mapping(
    {
        "api_keys": {
            "ada-key": {"principal_id": str(ADA)},
            "bob-key": {"principal_id": str(BOB)},
        }
    }
)

# The app's egress: one mailbox and one cursor store, shared by the SSE route and the emit
# hook below. A module-level pair because `MockApp` takes the factory and the hook as two
# separate callables — in a real app this is whatever your egress already uses.
MAILBOX = InMemoryRealtimeMailbox()
CURSORS = InMemoryMailboxCursors()


# --8<-- [start:app]
def build_app(runtime: ExecutionRuntime) -> FastAPI:
    """The app's own factory: an authenticated SSE endpoint, and nothing mock-specific.

    Identity is real — `SecurityContextMiddleware` with the app's local API keys — so the
    SSE route reads the principal from the bound context exactly as it does in production,
    and a stream serves the credential that opened it.
    """

    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=runtime.get_context,
        mailbox_factory=lambda _ctx: MAILBOX,
        cursors_factory=lambda _ctx: CURSORS,
    )

    app = FastAPI(title="Realtime SSE (mock)", lifespan=runtime_lifespan(runtime))
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(InvocationMetadataMiddleware, ctx_dep=runtime.get_context)
    app.add_middleware(
        SecurityContextMiddleware,
        ctx_dep=runtime.get_context,
        authn=AuthnRequirement(
            ingress=(HeaderApiKeyAuthn(authn_spec=AUTHN, header_name="X-API-Key", required=True),),
        ),
        when_multiple_credentials="first_in_order",
    )

    return app


# --8<-- [end:app]


# --8<-- [start:emit]
async def on_emit(ctx: ExecutionContext, signal: RealtimeSignal) -> None:
    """Deliver a commanded signal — the app's own egress, called by ``POST /_mock/emit``.

    Stored against the signal's audience, so a signal addressed to one principal lands in
    that principal's mailbox and in no one else's. The control plane never learns how
    delivery works; it only knows the app offered a way to do it.
    """

    _ = ctx

    await MAILBOX.store(
        principal=signal.audience_name,
        event_id=str(uuid7()),
        hlc=HlcTimestamp(physical_ms=_next_mark(), logical=0),
        signal=signal,
    )


def _next_mark() -> int:
    """A monotonic ordering mark, so replays come back in the order they were fired.

    A counter rather than the wall clock: two signals emitted in the same millisecond would
    otherwise share an HLC and replay in an arbitrary order, which is the kind of flake that
    only shows up on a fast machine.
    """

    global _mark
    _mark += 1

    return _mark


_mark = 0
# --8<-- [end:emit]


def reset_egress() -> None:
    """Empty the mailbox and the cursors.

    Worth knowing about the control plane: ``POST /_mock/reset`` clears ``MockState``, and
    this mailbox is not in it — it is the *app's* state, held by the app. Anything an app
    owns outside the mock's stores is the app's to reset, which is why this exists and why
    a served app with its own caches should offer the same.
    """

    global MAILBOX, CURSORS, _mark

    MAILBOX = InMemoryRealtimeMailbox()  # pyright: ignore[reportConstantRedefinition]
    CURSORS = InMemoryMailboxCursors()  # pyright: ignore[reportConstantRedefinition]
    _mark = 0


mock_app = MockApp(
    build_app=build_app,
    deps=(local_identity_deps(IDENTITY, authn_route=AUTHN.name),),
    on_emit=on_emit,
    # Both principals need a policy-principal document, or the app's own eligibility gate
    # refuses their key — see the mock-server recipe's note.
    seed=SeedPlan(
        specs=(
            spec_seed(
                policy_principal_spec,
                fixtures=({"id": str(ADA), "kind": "user"}, {"id": str(BOB), "kind": "user"}),
            ),
        )
    ),
)
