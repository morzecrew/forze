"""Effect factory that invokes another usecase with mappers."""

from enum import StrEnum
from typing import Callable

import attrs

from .context import ExecutionContext
from .middleware import Effect
from .plan import DispatchDeclaringEffectFactory, OpKey
from .registry import UsecaseRegistry

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecaseDelegate[PArgs, PRes, CArgs, CRes]:
    """Declares how to run a child usecase as an effect of a parent.

    Use :meth:`effect_factory` with :class:`~forze.application.execution.plan.UsecasePlan`
    ``after`` / ``in_tx_after`` / ``after_commit`` (or pipelines): dispatch edges are
    derived for :meth:`UsecaseRegistry.finalize`. Use
    :meth:`UsecaseRegistry.add_dispatch_edge` for edges not expressed via this
    delegate (for example hand-written effects that call ``resolve``).
    """

    target_op: str | StrEnum
    """Logical child operation key (same string form as registry registration)."""

    map_in: Callable[[PArgs, PRes], CArgs]
    """Build child usecase args from parent args and parent result."""

    map_out: Callable[[PArgs, PRes, CArgs, CRes], PRes] | None = None
    """Optional merge parent and child outcomes into the parent result."""

    # ....................... #

    def effect_factory(
        self,
        registry: UsecaseRegistry,
    ) -> DispatchDeclaringEffectFactory:
        """Build an :class:`~forze.application.execution.middleware.Effect` factory.

        The ``registry`` object must be the same instance that is later
        :meth:`~UsecaseRegistry.finalize`d (for example by building the registry
        with ``inplace=True``), or the captured reference must already carry a
        registry id when effects run.
        """

        delegate = self

        def inner(ctx: ExecutionContext) -> Effect[PArgs, PRes]:
            async def effect(args: PArgs, res: PRes) -> PRes:
                child = registry.resolve(str(delegate.target_op), ctx)
                child_args = delegate.map_in(args, res)
                child_res = await child(child_args)

                if delegate.map_out is not None:
                    return delegate.map_out(
                        args,
                        res,
                        child_args,
                        child_res,
                    )

                return res

            return effect

        return DispatchDeclaringEffectFactory(
            inner=inner,
            dispatch_targets=frozenset({str(delegate.target_op)}),
        )


# ....................... #


def delegated_usecase_effect[PArgs, PRes, CArgs, CRes](
    registry: UsecaseRegistry,
    target_op: OpKey,
    map_in: Callable[[PArgs, PRes], CArgs],
    map_out: Callable[[PArgs, PRes, CArgs, CRes], PRes] | None = None,
) -> DispatchDeclaringEffectFactory:
    """Shorthand for :class:`UsecaseDelegate` without storing the helper object."""

    return UsecaseDelegate[PArgs, PRes, CArgs, CRes](
        target_op=str(target_op),
        map_in=map_in,
        map_out=map_out,
    ).effect_factory(registry)
