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
    routed_inngest_lifecycle_step,
)
from .kernel.client import (
    InngestClient,
    InngestClientPort,
    InngestConfig,
    InngestRoutingCredentials,
    RoutedInngestClient,
)

__all__ = [
    "InngestClient",
    "InngestClientPort",
    "InngestConfig",
    "RoutedInngestClient",
    "InngestRoutingCredentials",
    "InngestClientDepKey",
    "InngestDepsModule",
    "InngestEventConfig",
    "InngestFunctionBinding",
    "InngestFunctionConfig",
    "get_function_bindings",
    "inngest_lifecycle_step",
    "routed_inngest_lifecycle_step",
    "register_functions",
]
