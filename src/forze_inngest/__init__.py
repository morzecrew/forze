"""Inngest integration for Forze durable function contracts."""

from ._compat import require_inngest

require_inngest()

# ....................... #

from .execution import (
    InngestClientDepKey,
    InngestDepsModule,
    InngestEventConfig,
    InngestFunctionBinding,
    InngestFunctionConfig,
    get_function_bindings,
    inngest_lifecycle_step,
    register_functions,
)
from .kernel.client import (
    InngestClient,
    InngestClientPort,
    InngestConfig,
    InngestRoutingCredentials,
    RoutedInngestClient,
)
from .settings import InngestSettings

__all__ = [
    "InngestClient",
    "InngestClientPort",
    "InngestConfig",
    "InngestSettings",
    "RoutedInngestClient",
    "InngestRoutingCredentials",
    "InngestClientDepKey",
    "InngestDepsModule",
    "InngestEventConfig",
    "InngestFunctionBinding",
    "InngestFunctionConfig",
    "get_function_bindings",
    "inngest_lifecycle_step",
    "register_functions",
]
