"""Container-backed sandbox execution: the first tier that can hold unreviewed code.

Wires :class:`ContainerSandboxDepsModule` with one :class:`ContainerSandboxConfig` per
route. The adapter speaks the Docker Engine API over a socket, so a Podman daemon serves it
unchanged; what it needs from either is an image that is already there, because nothing here
pulls one at the moment of a request.
"""

from forze_sandbox.container._compat import require_sandbox_container

require_sandbox_container()

# ....................... #

from .adapters import ConfigurableContainerSandbox, ContainerSandbox
from .execution import ContainerSandboxDepsModule, validate_container_route
from .kernel import (
    CONTAINER_BACKEND,
    DEFAULT_DOCKER_HOST,
    DEFAULT_RUN_AS,
    ContainerEngine,
    ContainerNotCreated,
    ContainerSandboxConfig,
    container_capabilities,
)

# ----------------------- #

__all__ = [
    "CONTAINER_BACKEND",
    "DEFAULT_DOCKER_HOST",
    "DEFAULT_RUN_AS",
    "ConfigurableContainerSandbox",
    "ContainerEngine",
    "ContainerNotCreated",
    "ContainerSandbox",
    "ContainerSandboxConfig",
    "ContainerSandboxDepsModule",
    "container_capabilities",
    "validate_container_route",
]
