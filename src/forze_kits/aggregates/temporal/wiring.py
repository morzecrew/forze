"""The reusable temporal-validity wiring for one document aggregate."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.base.exceptions import exc

from .factories import build_temporal_registry
from .policy import TemporalPolicy, assert_guarantee

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.querying import QueryFilterExpression
    from forze.application.execution.operations.registry import OperationRegistry
    from forze.base.primitives import StrKeyNamespace

# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class TemporalWiring:
    """The reusable temporal-validity wiring for one document aggregate.

    Only :meth:`bind`, and only merging: effective dating adds two questions and changes no
    answer, so unlike the versioned arm there is nothing here overriding a generated operation
    and nothing on the mapper slots. The write-side rule lives on the domain model, which is
    the only place that sees the period an update produces.
    """

    spec: DocumentSpec[Any, Any, Any, Any]
    """The temporal document aggregate (domain + create cmd on the validity mixins)."""

    policy: TemporalPolicy
    """The key a period is scoped by, and which endpoints are in force."""

    restrict: tuple[QueryFilterExpression, ...] = ()
    """What the aggregate's other arms exclude from every read.

    The dated reads build their own filter instead of passing through the mapper the generated
    reads share, so nothing reaches them implicitly: a composed aggregate has to hand its
    restrictions over, or it answers "what is in force" with a row it hides everywhere else."""

    # ....................... #

    def ops(self, *, ns: StrKeyNamespace | None = None) -> OperationRegistry:
        """The EFFECTIVE_ON + TIMELINE reads."""

        return build_temporal_registry(self.spec, self.policy, restrict=self.restrict, ns=ns)

    # ....................... #

    def bind(
        self,
        reg: OperationRegistry,
        *,
        ns: StrKeyNamespace | None = None,
    ) -> OperationRegistry:
        """Merge the two dated reads into *reg*."""

        return type(reg).merge(reg, self.ops(ns=ns or self.spec.default_namespace))


# ....................... #


def temporal_wiring(
    spec: DocumentSpec[Any, Any, Any, Any],
    policy: TemporalPolicy,
    *,
    restrict: tuple[QueryFilterExpression, ...] = (),
) -> TemporalWiring:
    """Build the reusable temporal-validity wiring for *spec*.

    Refuses at construction on two counts, each of which would otherwise surface as a wrong
    answer rather than an error: a spec that does not declare the matching non-overlap
    guarantee, and a domain model whose convention differs from the policy's.

    Nothing here checks that the read model exposes the validity dates or the key. That is not
    an oversight and not a gap: a temporal aggregate must declare the guarantee, the guarantee
    names exactly those fields, and :class:`~forze.application.contracts.document.DocumentSpec`
    refuses at construction when a guarantee names a field the aggregate does not store. A
    check here could only fire for a spec that cannot be built.
    """

    assert_guarantee(spec, policy)
    _assert_bounds_agree(spec, policy)

    return TemporalWiring(spec=spec, policy=policy, restrict=restrict)


# ....................... #


def _assert_bounds_agree(
    spec: DocumentSpec[Any, Any, Any, Any],
    policy: TemporalPolicy,
) -> None:
    """Refuse a domain model whose convention differs from the policy's.

    The convention is stated twice — on the policy, which the reads and the guarantee read, and
    on the model, which is the only place that can see the period an update produces. Two
    statements of one fact drift, so the one that would let them is refused: a model reading
    ``"[]"`` under a ``"[)"`` policy accepts a row the reads treat as covering nothing.

    :raises CoreException: ``configuration`` naming both values.
    """

    if spec.write is None:
        return

    declared = getattr(spec.write["domain"], "temporal_bounds", None)

    if declared is None or declared == policy.bounds:
        return

    raise exc.configuration(
        f"Document {spec.name!r} declares bounds {policy.bounds!r} on its temporal policy and "
        f"{declared!r} on its domain model. The policy's value is what the dated reads and the "
        "store's guarantee use; the model's is what decides which periods may be written. With "
        "the two disagreeing, a row the model accepts is one the reads treat as covering no "
        f"day. Set `temporal_bounds = {policy.bounds!r}` on the domain model.",
        details={
            "document": str(spec.name),
            "policy": policy.bounds,
            "model": str(declared),
        },
    )


# ....................... #

__all__ = ["TemporalWiring", "temporal_wiring"]
