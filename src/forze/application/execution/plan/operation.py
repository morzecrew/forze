"""Per-operation middleware composition."""

from __future__ import annotations

from typing import Any, Iterable, Self, cast, final

import attrs

from forze.application._logger import logger
from forze.application.execution.bucket import ALL_BUCKETS, Bucket, coerce_bucket
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError

from .spec import MiddlewareSpec, TransactionSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationPlan:
    """Per-operation middleware composition with transaction support.

    Buckets: ``outer_*`` run outside :class:`TxMiddleware`; ``outer_finally`` and
    ``outer_on_failure`` are placed after ``outer_wrap`` and wrap the
    transactional segment (or core usecase when tx is disabled). ``in_tx_*``
    run inside the transaction scope. ``after_commit`` runs only after a
    successful commit.
    """

    tx: TransactionSpec | None = attrs.field(default=None)
    """Transaction spec for the operation. None means non-transactional."""

    outer_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    outer_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    outer_finally: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    outer_on_failure: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    outer_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)

    in_tx_before: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    in_tx_finally: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    in_tx_on_failure: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    in_tx_wrap: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)
    in_tx_after: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)

    after_commit: tuple[MiddlewareSpec, ...] = attrs.field(factory=tuple)

    # ....................... #

    def add(
        self,
        bucket: Bucket | str,
        spec: MiddlewareSpec,
    ) -> Self:
        """Add a middleware spec to a bucket."""

        b = coerce_bucket(bucket)
        key = b.value

        logger.trace(
            "Adding middleware spec to bucket '%s' (priority=%s, factory_id=%s)",
            key,
            spec.priority,
            id(spec.factory),
        )

        if not hasattr(self, key):
            raise CoreError(f"Invalid bucket: {key}")

        cur = getattr(self, key)

        logger.trace("Current bucket size: %s", len(cur))

        return attrs.evolve(self, **cast(dict[str, Any], {key: (*cur, spec)}))

    # ....................... #

    def validate(self) -> None:
        """Validate that in-tx buckets are only used when tx is enabled."""

        if (
            self.in_tx_before
            or self.in_tx_after
            or self.in_tx_wrap
            or self.in_tx_finally
            or self.in_tx_on_failure
            or self.after_commit
        ) and self.tx is None:
            raise CoreError(
                "Operation plan uses IN_TX_* middlewares but tx() is not enabled"
            )

    # ....................... #

    def __ensure_no_collisions(
        self,
        specs: Iterable[MiddlewareSpec],
        *,
        bucket: Bucket | str,
    ) -> None:
        b = coerce_bucket(bucket)
        used: set[int] = set()

        for s in specs:
            k = s.priority

            if k in used:
                raise CoreError(
                    f"Priority collision in bucket '{b.value}': {s.priority}"
                )

            used.add(k)

    # ....................... #

    def __dedupe(self, bucket: Bucket | str) -> tuple[MiddlewareSpec, ...]:
        b = coerce_bucket(bucket)
        key = b.value

        if not hasattr(self, key):
            raise CoreError(f"Invalid bucket: {key}")

        cur = getattr(self, key)
        seen: set[tuple[int, int]] = set()
        out: list[MiddlewareSpec] = []

        for s in cur:
            k = (id(s.factory), s.priority)

            if k in seen:
                continue

            seen.add(k)
            out.append(s)

        self.__ensure_no_collisions(out, bucket=b)

        return tuple(out)

    # ....................... #

    def __sort(
        self,
        specs: Iterable[MiddlewareSpec],
        *,
        reverse: bool,
    ) -> tuple[MiddlewareSpec, ...]:
        return tuple(sorted(specs, key=lambda s: s.priority, reverse=reverse))

    # ....................... #

    def build(self, bucket: Bucket | str) -> tuple[MiddlewareSpec, ...]:
        """Build the ordered middleware specs for a bucket."""

        deduped_specs = self.__dedupe(bucket)
        return self.__sort(deduped_specs, reverse=True)

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc]
        cls: type[Self],  # pyright: ignore[reportGeneralTypeIssues]
        *plans: Self,
    ) -> OperationPlan:
        """Merge multiple plans into a single aggregate plan."""

        acc: OperationPlan = OperationPlan()

        for plan in plans:
            updates: dict[str, object] = {"tx": acc.tx or plan.tx}
            for b in ALL_BUCKETS:
                k = b.value
                updates[k] = (*getattr(acc, k), *getattr(plan, k))
            acc = attrs.evolve(acc, **cast(dict[str, Any], updates))

        return acc

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # pyright: ignore[reportUnusedFunction]
        self: Self,
        *plans: Self,
    ) -> OperationPlan:
        return type(self).merge(self, *plans)
