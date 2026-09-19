"""Factories for temporal-validity document registries."""

from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.dto.paginated import Paginated

from .dto import EffectiveOnDTO, TimelineDTO
from .handlers import EffectiveOn, Timeline
from .operations import TemporalKernelOp
from .policy import TemporalPolicy

# ----------------------- #


def _parametrized(generic: Any, arg: Any) -> Any:
    """Parametrize a generic envelope with a runtime model type.

    Kept off the static-type path: the models are only known at build time, so the subscription
    happens on values rather than as an annotation.
    """

    return generic[arg]


# ....................... #


def build_temporal_registry(
    spec: DocumentSpec[Any, Any, Any, Any],
    policy: TemporalPolicy,
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build the two effective-dated reads for *spec*.

    Both are reads, so unlike the versioned kit there is nothing here that overrides a generated
    write — the temporal arm adds operations and changes none.

    :param spec: The temporal document specification.
    :param policy: The key a period is scoped by, and the convention.
    :param ns: Optional namespace.
    :returns: Operation registry with EFFECTIVE_ON and TIMELINE.
    """

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(TemporalKernelOp.EFFECTIVE_ON): lambda ctx: EffectiveOn(
                query=ctx.doc.query(spec),
                policy=policy,
            ),
            ns.key(TemporalKernelOp.TIMELINE): lambda ctx: Timeline(
                query=ctx.doc.query(spec),
                policy=policy,
            ),
        },
    )

    return reg.set_descriptors(
        {
            TemporalKernelOp.EFFECTIVE_ON: OperationDescriptor(
                input_type=EffectiveOnDTO,
                output_type=spec.read,
                description="The row in force for one key on a given day.",
                sensitive=spec.sensitive,
            ),
            TemporalKernelOp.TIMELINE: OperationDescriptor(
                input_type=TimelineDTO,
                output_type=_parametrized(Paginated, spec.read),
                description="Every row for one key whose period meets a window.",
                sensitive=spec.sensitive,
            ),
        },
        namespace=ns,
    )
