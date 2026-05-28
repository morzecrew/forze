"""Inngest dependency module for the application kernel."""

from collections.abc import Sequence
from enum import StrEnum
from typing import Any, Mapping, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionStepDepKey,
    DurableFunctionStepPort,
)
from forze.application.execution import Deps, DepsModule

from ...adapters import InngestStepAdapter
from ..registration import InngestFunctionBinding
from ...kernel.platform import InngestClientPort
from .configs import InngestEventConfig
from .deps import ConfigurableInngestEventCommand
from .keys import InngestClientDepKey

# ----------------------- #


def _provide_step_port(_ctx: Any) -> DurableFunctionStepPort:
    return InngestStepAdapter()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class InngestDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Inngest client and durable function ports."""

    client: InngestClientPort
    """Pre-constructed Inngest client."""

    events: Mapping[K, InngestEventConfig] | None = attrs.field(default=None)
    """Mapping from event spec names to Inngest event command configuration."""

    function_bindings: Sequence[InngestFunctionBinding[Any, Any]] | None = attrs.field(
        default=None,
    )
    """Function bindings used by :func:`~forze_inngest.fastapi.serve.serve` (not resolved via deps)."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        plain: dict[DepKey[Any], Any] = {
            InngestClientDepKey: self.client,
            DurableFunctionStepDepKey: _provide_step_port,
        }

        plain_deps = Deps[K].plain(plain)
        event_deps = Deps[K]()

        if self.events:
            event_deps = event_deps.merge(
                Deps[K].routed(
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
    module: InngestDepsModule[Any],
) -> Sequence[InngestFunctionBinding[Any, Any]]:
    """Return function bindings stored on an :class:`InngestDepsModule`."""

    return module.function_bindings or ()
