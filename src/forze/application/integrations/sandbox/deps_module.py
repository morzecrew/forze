"""Wiring for subprocess sandbox routes — and the gates that fail the boot.

The gates live here because here is where they *can* live: a route's config is known at
freeze, a handler's spec is not. §5's four gates are one function, run once per route, and
a failure is a failed boot rather than a failed request — which is the whole point of
declaring provenance in the first place.
"""

from typing import final

import attrs

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.sandbox import SandboxDepKey, validate_provenance
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from .process import (
    SUBPROCESS_BACKEND,
    SUBPROCESS_CAPABILITIES,
    ConfigurableSubprocessSandbox,
    SubprocessSandboxConfig,
)

# ----------------------- #


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
        capabilities=SUBPROCESS_CAPABILITIES,
        backend=SUBPROCESS_BACKEND,
        route=route,
    )

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
