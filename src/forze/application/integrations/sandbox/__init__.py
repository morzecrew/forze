"""The base sandbox adapter and its wiring — governed child processes, no isolation.

Ships the plane's weakest honest tier: a child that is killed when it must be, whose files
cross by storage key, whose output is bounded, and whose workspace is cleaned on every exit
path. It declares ``isolation="none"``, so the provenance gate refuses it every route that
runs code nobody reviewed — which is the plane working rather than a gap in it.
"""

from .deps_module import (
    SubprocessSandboxDepsModule,
)
from .process import (
    SUBPROCESS_BACKEND,
    SUBPROCESS_CAPABILITIES,
    ConfigurableSubprocessSandbox,
    SubprocessSandbox,
    SubprocessSandboxConfig,
    subprocess_capabilities,
)

# ----------------------- #

__all__ = [
    "SUBPROCESS_BACKEND",
    "SUBPROCESS_CAPABILITIES",
    "subprocess_capabilities",
    "ConfigurableSubprocessSandbox",
    "SubprocessSandbox",
    "SubprocessSandboxConfig",
    "SubprocessSandboxDepsModule",
]
