"""The deps module that registers container sandbox routes."""

from .module import ContainerSandboxDepsModule, validate_container_route

# ----------------------- #

__all__ = [
    "ContainerSandboxDepsModule",
    "validate_container_route",
]
