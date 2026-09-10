"""Wiring for container sandbox routes — and the gates that fail the boot.

The gates live here because here is where they *can* live: a route's config is known at
freeze, a handler's spec is not. A failure is a failed boot rather than a failed request,
which is the whole point of declaring provenance in the first place.

This is the tier where the plane's headline refusal finally has a passing side. Every
adapter below it fails ``provenance="untrusted"`` by construction; a route wired here is the
one an application can actually run unreviewed code on.
"""

from typing import final

import attrs

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.sandbox import SandboxDepKey, validate_provenance
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...adapters.sandbox import ConfigurableContainerSandbox
from ...kernel.config import CONTAINER_BACKEND, ContainerSandboxConfig, container_capabilities

# ----------------------- #


def validate_container_route(*, route: str, config: ContainerSandboxConfig) -> None:
    """Refuse a container route that cannot honestly carry what it declares.

    Two refusals, both fail-closed and both at freeze. The provenance gate is the plane's
    own, unchanged — it passes here, which no adapter below this tier can say. The egress
    acknowledgment is narrower than the subprocess adapter's: that one is owed
    unconditionally because a bare child inherits the host's connectivity whatever anyone
    wires, while this adapter really can close the network and does so by default. A route
    that opens it is making a choice, and says so.

    The remaining §5 gates — ceilings, the image, a non-root identity — are enforced by
    :class:`ContainerSandboxConfig` itself, since a config without them cannot be built.
    """

    validate_provenance(
        provenance=config.provenance,
        capabilities=container_capabilities(config),
        backend=CONTAINER_BACKEND,
        route=route,
    )

    if config.network == "egress" and not config.acknowledge_network_egress:
        raise exc.configuration(
            f"Sandbox route {route!r} opens the network for its containers. With staged "
            "inputs in the workspace, network access is an egress path for them, and this "
            "route runs code the isolation tier exists to distrust. Set "
            "acknowledge_network_egress=True to state that you know what the child can "
            "reach, or leave network='none' and keep it closed.",
            code="sandbox_network_egress_unacknowledged",
            details={"route": route},
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ContainerSandboxDepsModule(DepsModule):
    """Register container sandbox routes: one :class:`ContainerSandboxConfig` per route.

    Route names are the ``SandboxSpec.name`` values handlers resolve.
    """

    routes: StrKeyMapping[ContainerSandboxConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-route container configs, keyed by spec name."""

    # ....................... #

    def __call__(self) -> Deps:
        for route, config in self.routes.items():
            validate_container_route(route=str(route), config=config)

        return Deps.routed(
            {
                SandboxDepKey: {
                    name: ConfigurableContainerSandbox(config=config)
                    for name, config in self.routes.items()
                },
            },
        )
