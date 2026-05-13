"""Middleware placement buckets and per-bucket scheduling metadata."""

from __future__ import annotations

from enum import StrEnum
from typing import Final, Literal, Mapping

import attrs

from forze.application.execution.plan_kinds import StepExplainKind
from forze.base.errors import CoreError

# ----------------------- #

BucketPhase = Literal["outer", "in_tx", "after_commit"]
"""Where the bucket runs relative to :class:`~forze.application.execution.middleware.TxMiddleware`."""

MiddlewareShape = Literal["guard", "effect", "wrap", "finally", "on_failure"]
"""What kind of factory/wrapper the bucket holds for plan builders."""

# ....................... #


class Bucket(StrEnum):
    """Placement bucket for middleware specs on an :class:`~forze.application.execution.plan.OperationPlan`."""

    outer_before = "outer_before"
    outer_wrap = "outer_wrap"
    outer_finally = "outer_finally"
    outer_on_failure = "outer_on_failure"
    outer_after = "outer_after"
    in_tx_before = "in_tx_before"
    in_tx_finally = "in_tx_finally"
    in_tx_on_failure = "in_tx_on_failure"
    in_tx_wrap = "in_tx_wrap"
    in_tx_after = "in_tx_after"
    after_commit = "after_commit"


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BucketMeta:
    """Static rules for one :class:`Bucket`."""

    phase: BucketPhase
    """Whether the bucket is outside tx, inside tx, or runs after commit."""

    reverse_for_usecase_tuple: bool
    """When ``True``, :func:`middleware_specs_for_usecase_tuple` reverses :meth:`OperationPlan.build` order."""

    capability_schedulable: bool
    """When ``True``, :func:`~forze.application.execution.capabilities.schedule_capability_specs` may reorder."""

    middleware_shape: MiddlewareShape
    """Shape used by :class:`~forze.application.execution.plan.UsecasePlan` builders."""

    explain_kind: StepExplainKind
    """``kind`` field for :class:`~forze.application.execution.plan.StepExplainRow` (never ``tx``)."""


# ....................... #


def _meta(
    *,
    phase: BucketPhase,
    reverse_for_usecase_tuple: bool,
    capability_schedulable: bool,
    middleware_shape: MiddlewareShape,
    explain_kind: StepExplainKind,
) -> BucketMeta:
    return BucketMeta(
        phase=phase,
        reverse_for_usecase_tuple=reverse_for_usecase_tuple,
        capability_schedulable=capability_schedulable,
        middleware_shape=middleware_shape,
        explain_kind=explain_kind,
    )


# ....................... #


BUCKET_REGISTRY: Final[Mapping[Bucket, BucketMeta]] = {
    Bucket.outer_before: _meta(
        phase="outer",
        reverse_for_usecase_tuple=False,
        capability_schedulable=True,
        middleware_shape="guard",
        explain_kind="guard",
    ),
    Bucket.outer_wrap: _meta(
        phase="outer",
        reverse_for_usecase_tuple=True,
        capability_schedulable=False,
        middleware_shape="wrap",
        explain_kind="wrap",
    ),
    Bucket.outer_finally: _meta(
        phase="outer",
        reverse_for_usecase_tuple=True,
        capability_schedulable=False,
        middleware_shape="finally",
        explain_kind="finally",
    ),
    Bucket.outer_on_failure: _meta(
        phase="outer",
        reverse_for_usecase_tuple=True,
        capability_schedulable=False,
        middleware_shape="on_failure",
        explain_kind="on_failure",
    ),
    Bucket.outer_after: _meta(
        phase="outer",
        reverse_for_usecase_tuple=True,
        capability_schedulable=True,
        middleware_shape="effect",
        explain_kind="effect",
    ),
    Bucket.in_tx_before: _meta(
        phase="in_tx",
        reverse_for_usecase_tuple=False,
        capability_schedulable=True,
        middleware_shape="guard",
        explain_kind="guard",
    ),
    Bucket.in_tx_finally: _meta(
        phase="in_tx",
        reverse_for_usecase_tuple=True,
        capability_schedulable=False,
        middleware_shape="finally",
        explain_kind="finally",
    ),
    Bucket.in_tx_on_failure: _meta(
        phase="in_tx",
        reverse_for_usecase_tuple=True,
        capability_schedulable=False,
        middleware_shape="on_failure",
        explain_kind="on_failure",
    ),
    Bucket.in_tx_wrap: _meta(
        phase="in_tx",
        reverse_for_usecase_tuple=True,
        capability_schedulable=False,
        middleware_shape="wrap",
        explain_kind="wrap",
    ),
    Bucket.in_tx_after: _meta(
        phase="in_tx",
        reverse_for_usecase_tuple=True,
        capability_schedulable=True,
        middleware_shape="effect",
        explain_kind="effect",
    ),
    Bucket.after_commit: _meta(
        phase="after_commit",
        reverse_for_usecase_tuple=False,
        capability_schedulable=True,
        middleware_shape="effect",
        explain_kind="effect",
    ),
}

# ....................... #

ALL_BUCKETS: Final[tuple[Bucket, ...]] = tuple(BUCKET_REGISTRY.keys())

# ....................... #


def coerce_bucket(bucket: Bucket | str) -> Bucket:
    """Return :class:`Bucket` for ``bucket`` (enum member or string value)."""

    if isinstance(bucket, Bucket):
        return bucket

    try:
        return Bucket(bucket)
    except ValueError as e:
        raise CoreError(f"Invalid bucket: {bucket!r}") from e


# ....................... #

CAPABILITY_SCHEDULABLE_BUCKETS: Final[frozenset[Bucket]] = frozenset(
    b for b, m in BUCKET_REGISTRY.items() if m.capability_schedulable
)

DISPATCH_EDGE_BUCKETS: Final[frozenset[Bucket]] = frozenset(
    b for b, m in BUCKET_REGISTRY.items() if m.middleware_shape == "effect"
)

# ....................... #


def iter_capability_schedulable_buckets() -> tuple[Bucket, ...]:
    """Buckets passed to the capability scheduler (finalize + explain + chain)."""

    return tuple(
        sorted(CAPABILITY_SCHEDULABLE_BUCKETS, key=lambda b: ALL_BUCKETS.index(b))
    )
