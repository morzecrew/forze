"""Wiring for subprocess sandbox routes — and the gates that fail the boot.

The gates live here because here is where they *can* live: a route's config is known at
freeze, a handler's spec is not. §5's four gates are one function, run once per route, and
a failure is a failed boot rather than a failed request — which is the whole point of
declaring provenance in the first place.
"""

import os
import sys
from typing import final

import attrs

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.sandbox import SandboxDepKey, validate_provenance
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from .process import (
    SUBPROCESS_BACKEND,
    ConfigurableSubprocessSandbox,
    SubprocessSandboxConfig,
    _as_gid,  # pyright: ignore[reportPrivateUsage]
    _as_uid,  # pyright: ignore[reportPrivateUsage]
    subprocess_capabilities,
)

# ----------------------- #


def _refuse_a_drop_this_worker_cannot_make(*, route: str, config: SubprocessSandboxConfig) -> None:
    """Refuse a privilege drop at freeze when this worker could not make it.

    Refused here rather than at the first call, matching every other gate: a route that
    cannot honour what it declares should be wrong once, at startup, instead of on every
    request.

    Dropping to the identity the worker already has is not a drop and needs no privileges,
    so it is allowed — that is the case a test can actually exercise, and refusing it would
    leave the whole path unexercised while claiming to guard it.
    """

    if not hasattr(os, "geteuid"):
        raise exc.configuration(
            f"Sandbox route {route!r} asks to run its children as another user, and this "
            "platform has no process identity to change. Drop run_as_user / run_as_group, "
            "or wire this route where the child can be given one.",
            code="sandbox_privilege_drop_unavailable",
            details={"route": route, "platform": sys.platform},
        )

    try:
        wanted = (_as_uid(config.run_as_user), _as_gid(config.run_as_group))

    except KeyError as error:
        raise exc.configuration(
            f"Sandbox route {route!r} names a user or group this host does not have: "
            f"{error}. A route cannot become somebody who is not there.",
            code="sandbox_privilege_drop_unknown_identity",
            details={"route": route},
        ) from error

    unchanged = wanted[0] in (-1, os.getuid()) and wanted[1] in (-1, os.getgid())

    if unchanged or os.geteuid() == 0:
        return

    raise exc.configuration(
        f"Sandbox route {route!r} asks to run its children as another user, which requires "
        "this worker to be root, and it is not. Drop run_as_user / run_as_group, name the "
        "identity the worker already has, or run the worker somewhere it can set them.",
        code="sandbox_privilege_drop_unavailable",
        details={"route": route, "euid": os.geteuid(), "wanted": list(wanted)},
    )


def validate_subprocess_route(*, route: str, config: SubprocessSandboxConfig) -> None:
    """Refuse a subprocess route that cannot honestly carry what it declares.

    Two refusals, both fail-closed and both at freeze:

    1. **Untrusted provenance.** A bare child shares this host's filesystem, network and
       ``/proc``; nothing about it contains a program nobody reviewed.
    2. **Unacknowledged egress.** This adapter cannot stop the child reaching the network,
       so every route wired to it says so explicitly. A route that would rather not
       acknowledge it needs an adapter that can actually close the network.

    The remaining §5 gates — mandatory wall-clock and output ceilings — are enforced by
    :class:`SubprocessSandboxConfig` itself, since a config without them cannot be built.
    """

    validate_provenance(
        provenance=config.provenance,
        capabilities=subprocess_capabilities(config),
        backend=SUBPROCESS_BACKEND,
        route=route,
    )

    if config.drops_privileges:
        _refuse_a_drop_this_worker_cannot_make(route=route, config=config)

    if not config.acknowledge_network_egress:
        raise exc.configuration(
            f"Sandbox route {route!r} runs children that can reach the network: a bare "
            "subprocess inherits this host's connectivity and no setting here changes that. "
            "Set acknowledge_network_egress=True to state that you know what the child can "
            "reach — with staged inputs in its workspace, network access is an egress path "
            "for them — or wire an adapter that can close the network.",
            code="sandbox_network_egress_unacknowledged",
            details={"route": route},
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SubprocessSandboxDepsModule(DepsModule):
    """Register subprocess sandbox routes: one :class:`SubprocessSandboxConfig` per route.

    Route names are the ``SandboxSpec.name`` values handlers resolve.
    """

    routes: StrKeyMapping[SubprocessSandboxConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-route subprocess configs, keyed by spec name."""

    # ....................... #

    def __call__(self) -> Deps:
        for route, config in self.routes.items():
            validate_subprocess_route(route=str(route), config=config)

        return Deps.routed(
            {
                SandboxDepKey: {
                    name: ConfigurableSubprocessSandbox(config=config)
                    for name, config in self.routes.items()
                },
            },
        )
