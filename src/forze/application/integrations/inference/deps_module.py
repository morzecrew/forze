"""Deps wiring and lifecycle for the in-process local inference adapter."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.inference import InferenceDepKey, InferenceSpec
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from .local import LocalInferenceAdapter, LocalInferenceConfig, LocalModelHost

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableLocalInference:
    """Build a :class:`LocalInferenceAdapter` for a given spec (one factory per route)."""

    host: LocalModelHost

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: InferenceSpec[Any, Any],
    ) -> LocalInferenceAdapter[Any, Any]:
        _ = ctx
        return LocalInferenceAdapter(spec=spec, host=self.host)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalInferenceDepsModule(DepsModule):
    """Register in-process inference routes: one :class:`LocalInferenceConfig` per route.

    Route names are the ``InferenceSpec.name`` values handlers resolve. Each route owns one
    :class:`LocalModelHost` (the load-once model holder), created here so the loaded model
    is per-process while adapters stay per-scope. Pair with
    :func:`local_inference_lifecycle_step` to warm ``warm_on_startup`` routes at boot.
    """

    models: StrKeyMapping[LocalInferenceConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-route local model configs, keyed by spec name."""

    hosts: StrKeyMapping[LocalModelHost] = attrs.field(
        default=attrs.Factory(
            lambda self: {
                name: LocalModelHost(config=config) for name, config in self.models.items()
            },
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )
    """Per-route load-once model holders (derived; one per config)."""

    # ....................... #

    def __call__(self) -> Deps:
        return Deps.routed(
            {
                InferenceDepKey: {
                    name: ConfigurableLocalInference(host=host) for name, host in self.hosts.items()
                },
            },
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalInferenceWarmupHook(LifecycleHook):
    """Startup hook that loads every ``warm_on_startup`` model, failing boot closed.

    A loader error propagates and aborts startup: a service that would fail its first
    prediction should not come up. Routes with ``warm_on_startup=False`` are skipped and
    load lazily on first call.
    """

    hosts: tuple[LocalModelHost, ...]

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext") -> None:
        _ = ctx

        for host in self.hosts:
            if host.config.warm_on_startup:
                await host.model()


# ....................... #


def local_inference_lifecycle_step(
    module: LocalInferenceDepsModule,
    *,
    name: StrKey = "local_inference_warmup",
    depends_on: tuple[StrKey, ...] = (),
) -> LifecycleStep:
    """Lifecycle step warming *module*'s ``warm_on_startup`` models at startup.

    Process-local (loads into this replica's memory), so it needs no shared-state or
    singleton guards. There is no shutdown: a loaded model holds no external resources
    the process teardown does not already release.
    """

    return LifecycleStep(
        id=name,
        depends_on=depends_on,
        startup=LocalInferenceWarmupHook(hosts=tuple(module.hosts.values())),
    )
