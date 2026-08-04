"""What a user declares to get their app served on the mock."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, final

import attrs

from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.contracts.deps import DepsModule
    from forze.application.contracts.execution import LifecycleStep
    from forze.application.contracts.realtime import RealtimeSignal
    from forze.application.execution import ExecutionContext, ExecutionRuntime
    from forze_mock.adapters import MockState
    from forze_mock.seeding import SeedPlan

# ----------------------- #

type AppFactory = Callable[["ExecutionRuntime"], Any]
"""The app's own factory: a runtime in, a mounted ASGI app out."""

type EmitHook = Callable[["ExecutionContext", "RealtimeSignal"], Awaitable[None]]
"""How a signal reaches the app's realtime egress — supplied by the app, never by the mock."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ControlPlane:
    """Whether the ``/_mock`` routes are mounted, and where."""

    enabled: bool = True
    """Mount the control plane. ``False`` serves the app alone."""

    prefix: str = "/_mock"
    """Path the control routes live under."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.enabled:
            return

        if not self.prefix.startswith("/"):
            raise exc.configuration(f"Control plane prefix must start with '/': {self.prefix!r}")

        # The control plane is mounted *before* the app, so a root prefix matches every path
        # and the served app becomes unreachable — a config footgun that presents as a
        # totally broken server rather than as a bad prefix.
        if self.prefix == "/":
            raise exc.configuration(
                "Control plane prefix must not be '/': mounted at the root it would swallow "
                "every route of the served app. Use a distinct prefix, or enabled=False"
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockApp:
    """An app, plus what it takes to serve it on in-memory backends.

    Optional by design: the framework acquires no app-assembly seam. A user exposes one of
    these, and ``forze mock serve module:attr`` resolves it the same way ``forze dst`` does.
    """

    build_app: AppFactory
    """The app's **own** factory — the production one, parameterized by runtime.

    Everything transport- and identity-shaped comes from here: routes, middleware, exception
    handlers, the authn ingress. The mock server owns none of it, which is what keeps
    ``forze_mock`` from importing a transport — and what makes it impossible for the server
    to mint a principal. Dev identity is the app's own wiring (e.g.
    ``forze_identity.builtin.local`` reading a key file).
    """

    modules: Sequence[DepsModule] = ()
    """Real deps modules to keep. Empty = fully mocked.

    The mock composes *under* these as a fallback module, so "real Postgres for documents,
    mock for everything else" is this list — not a second wiring path.
    """

    deps: Sequence[Any] = ()
    """Extra registration blobs (``Deps``) to merge — e.g. an app's local identity wiring."""

    lifecycle: Sequence[LifecycleStep] = ()
    """Lifecycle steps to run for the server's lifetime (relays, consumers, warmups)."""

    state: MockState | None = None
    """The store every mock adapter shares. ``None`` builds a fresh one."""

    mock: Any = None
    """A pre-built ``MockDepsModule`` when its knobs matter (identity routes, transactions,
    programmed HTTP/inference registries). ``None`` builds a default one over :attr:`state`."""

    seed: SeedPlan | None = None
    """Applied once the runtime scope is open, and re-applied by ``POST /_mock/reset``."""

    control: ControlPlane = attrs.field(factory=ControlPlane)
    """Control-plane configuration."""

    on_emit: EmitHook | None = None
    """How ``POST /_mock/emit`` delivers a signal.

    Supplied by the app for the same reason ``build_app`` is: the realtime egress plane
    lives above ``forze_mock``, so the server cannot reach it. Unset, the route refuses with
    that instruction rather than pretending to deliver.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not callable(self.build_app):
            raise exc.configuration("MockApp.build_app must be callable")

        # Same fail-fast path as build_app: unchecked, a non-callable hook surfaces as an
        # internal error on the one request that tries to emit, long after the mistake.
        if self.on_emit is not None and not callable(self.on_emit):
            raise exc.configuration("MockApp.on_emit must be callable")

        if self.state is not None and self.mock is not None:
            raise exc.configuration(
                "Pass either 'state' or a pre-built 'mock' module, not both — a MockDepsModule "
                "already carries its state, and two sources would silently disagree"
            )
