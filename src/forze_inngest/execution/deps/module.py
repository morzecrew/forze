"""Inngest dependency module for the application kernel."""

from typing import Any, Sequence, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionStepDepKey,
    DurableFunctionStepPort,
)
from forze.application.contracts.tenancy import (
    TenantIsolationMode,
    validate_module_tenancy,
)
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...adapters import InngestStepAdapter
from ...kernel.client import InngestClientPort, RoutedInngestClient
from ..registration import InngestFunctionBinding
from .configs import InngestEventConfig
from .factories import ConfigurableInngestEventCommand
from .keys import InngestClientDepKey

# ----------------------- #


def _provide_step_port(_ctx: Any) -> DurableFunctionStepPort:
    return InngestStepAdapter()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class InngestDepsModule(DepsModule):
    """Dependency module that registers Inngest client and durable function ports."""

    client: InngestClientPort
    """Pre-constructed Inngest client."""

    events: StrKeyMapping[InngestEventConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from event spec names to Inngest event command configuration."""

    function_bindings: Sequence[InngestFunctionBinding[Any, Any]] | None = attrs.field(
        default=None,
    )
    """Function bindings used by :func:`~forze_inngest.fastapi.serve.serve` (not resolved via deps)."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Inngest event configs carry no row-level tenant flag (tenancy rides the event envelope),
    so a declared floor is met only by a routed per-tenant client (``database``); a shared
    client derives ``none`` and fails a declared floor closed.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="Inngest",
            client_is_routed=isinstance(self.client, RoutedInngestClient),
            groups=[],
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="inngest_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        plain: dict[DepKey[Any], Any] = {
            InngestClientDepKey: self.client,
            DurableFunctionStepDepKey: _provide_step_port,
        }

        plain_deps = Deps.plain(plain)
        event_deps = Deps()

        if self.events:
            event_deps = event_deps.merge(
                Deps.routed(
                    {
                        DurableFunctionEventCommandDepKey: {
                            name: ConfigurableInngestEventCommand(config=config)
                            for name, config in self.events.items()
                        },
                    },
                ),
            )

        return plain_deps.merge(event_deps)


# ....................... #


def get_function_bindings(
    module: InngestDepsModule,
) -> Sequence[InngestFunctionBinding[Any, Any]]:
    """Return function bindings stored on an :class:`InngestDepsModule`."""

    return module.function_bindings or ()
