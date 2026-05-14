"""Per-operation middleware composition."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Self, final

import attrs

from forze.application._logger import logger
from forze.application.execution.bucket import BucketKey, Phase
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError

from .spec import MiddlewareSpec, TransactionSpec

# ----------------------- #


def _default_buckets() -> dict[BucketKey, tuple[MiddlewareSpec, ...]]:
    return {k: () for k in BucketKey.iter_all()}


def _normalize_buckets(
    m: Mapping[BucketKey, tuple[MiddlewareSpec, ...]] | None,
) -> dict[BucketKey, tuple[MiddlewareSpec, ...]]:
    if m is None:
        return _default_buckets()

    unknown = frozenset(m) - frozenset(BucketKey.iter_all())
    if unknown:
        raise CoreError(f"Unknown bucket keys: {unknown!r}")

    return {k: tuple(m[k]) for k in BucketKey.iter_all()}


def _dedupe_specs(
    specs: Iterable[MiddlewareSpec],
    *,
    bucket_label: str,
) -> tuple[MiddlewareSpec, ...]:
    seen: set[tuple[int, int]] = set()
    out: list[MiddlewareSpec] = []

    for s in specs:
        k = (id(s.factory), s.priority)
        if k in seen:
            continue
        seen.add(k)
        out.append(s)

    used: set[int] = set()
    for s in out:
        if s.priority in used:
            raise CoreError(
                f"Priority collision in bucket '{bucket_label}': {s.priority}"
            )
        used.add(s.priority)

    return tuple(out)


def _sort_by_priority(
    specs: Iterable[MiddlewareSpec],
    *,
    reverse: bool,
) -> tuple[MiddlewareSpec, ...]:
    return tuple(sorted(specs, key=lambda s: s.priority, reverse=reverse))


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Per-operation middleware composition with transaction support.

    Buckets are keyed by :class:`~forze.application.execution.bucket.BucketKey`
    (``Phase`` × ``Slot``). ``outer_*`` run outside :class:`TxMiddleware`;
    ``outer_finally`` and ``outer_on_failure`` sit after ``outer_wrap`` and wrap
    the transactional segment (or core usecase when tx is disabled). ``in_tx_*``
    run inside the transaction scope. ``after_commit`` runs only after a
    successful commit.
    """

    tx: TransactionSpec | None = attrs.field(default=None)
    """Transaction spec for the operation. None means non-transactional."""

    buckets: dict[BucketKey, tuple[MiddlewareSpec, ...]] = attrs.field(
        factory=_default_buckets,
        converter=_normalize_buckets,
    )
    """Specs per placement bucket (all :class:`BucketKey` members are present)."""

    # ....................... #

    def specs(self, key: BucketKey) -> tuple[MiddlewareSpec, ...]:
        """Return stored specs for ``key`` (append order, may contain duplicates)."""

        return self.buckets[key]

    def add(self, key: BucketKey, spec: MiddlewareSpec) -> Self:
        """Add a middleware spec to a bucket."""

        label = key.label
        logger.trace(
            "Adding middleware spec to bucket '%s' (priority=%s, factory_id=%s)",
            label,
            spec.priority,
            id(spec.factory),
        )

        cur = self.buckets[key]
        logger.trace("Current bucket size: %s", len(cur))

        new_buckets = dict(self.buckets)
        new_buckets[key] = (*cur, spec)
        return attrs.evolve(self, buckets=new_buckets)

    # ....................... #

    def validate(self) -> None:
        """Validate that in-tx and after-commit buckets are only used when tx is enabled."""

        if self.tx is not None:
            return

        for key, specs in self.buckets.items():
            if not specs:
                continue
            if key.phase is Phase.in_tx or key.phase is Phase.after_commit:
                raise CoreError(
                    "Operation plan uses IN_TX_* or after_commit middlewares but tx() is not enabled"
                )

    # ....................... #

    def build(self, key: BucketKey) -> tuple[MiddlewareSpec, ...]:
        """Dedupe, ensure unique priorities, sort descending by priority."""

        deduped = _dedupe_specs(self.buckets[key], bucket_label=key.label)
        return _sort_by_priority(deduped, reverse=True)

    def specs_for_chain(self, key: BucketKey) -> tuple[MiddlewareSpec, ...]:
        """Specs in the order consumed when composing the middleware chain."""

        ordered = self.build(key)
        if key.reverse_for_usecase_tuple:
            return tuple(reversed(ordered))
        return ordered

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc]
        cls: type[Self],  # pyright: ignore[reportGeneralTypeIssues]
        *plans: Self,
    ) -> OperationPlan:
        """Merge multiple plans into a single aggregate plan."""

        acc = OperationPlan()

        for plan in plans:
            new_buckets = {
                k: (*acc.buckets[k], *plan.buckets[k]) for k in BucketKey.iter_all()
            }
            acc = attrs.evolve(acc, tx=acc.tx or plan.tx, buckets=new_buckets)

        return acc

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # pyright: ignore[reportUnusedFunction]
        self: Self,
        *plans: Self,
    ) -> OperationPlan:
        return type(self).merge(self, *plans)
