"""Internal immutable stage model for one operation."""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from typing import Self, final

import attrs

from forze.base.errors import CoreError

from ..plan.spec import MiddlewareSpec, TransactionSpec
from .stages import Stage

# ----------------------- #


def _dedupe_specs(
    specs: Iterable[MiddlewareSpec],
    *,
    stage_label: str,
) -> tuple[MiddlewareSpec, ...]:
    seen: set[tuple[int, int]] = set()
    out: list[MiddlewareSpec] = []

    for spec in specs:
        key = (id(spec.factory), spec.priority)
        if key in seen:
            continue

        seen.add(key)
        out.append(spec)

    used: set[int] = set()
    for spec in out:
        if spec.priority in used:
            raise CoreError(
                f"Priority collision in stage '{stage_label}': {spec.priority}",
            )

        used.add(spec.priority)

    return tuple(out)


def _sort_by_priority(specs: Iterable[MiddlewareSpec]) -> tuple[MiddlewareSpec, ...]:
    return tuple(sorted(specs, key=lambda spec: spec.priority, reverse=True))


def _merge_stage_tuple(
    left: tuple[MiddlewareSpec, ...],
    right: tuple[MiddlewareSpec, ...],
) -> tuple[MiddlewareSpec, ...]:
    return (*left, *right)


def _copy_stage_specs(
    value: dict[Stage, tuple[MiddlewareSpec, ...]],
) -> dict[Stage, tuple[MiddlewareSpec, ...]]:
    return {stage: tuple(specs) for stage, specs in value.items()}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationStages:
    """Immutable middleware layout for one resolved operation."""

    tx_route: str | None = None
    stage_specs: dict[Stage, tuple[MiddlewareSpec, ...]] = attrs.field(
        factory=dict,
        converter=_copy_stage_specs,
    )

    # ....................... #

    def specs(self, stage: Stage) -> tuple[MiddlewareSpec, ...]:
        return self.stage_specs.get(stage, ())

    # ....................... #

    def add(self, stage: Stage, spec: MiddlewareSpec) -> Self:
        updated = dict(self.stage_specs)
        updated[stage] = (*self.specs(stage), spec)

        return attrs.evolve(self, stage_specs=updated)

    # ....................... #

    def with_tx(self, route: str | StrEnum) -> Self:
        return attrs.evolve(self, tx_route=str(route))

    # ....................... #

    @property
    def has_transaction(self) -> bool:
        return self.tx_route is not None

    # ....................... #

    @property
    def tx(self) -> TransactionSpec | None:
        if self.tx_route is None:
            return None

        return TransactionSpec(route=self.tx_route)

    # ....................... #

    def validate(self) -> None:
        if self.tx_route is not None:
            return

        if self.specs(Stage.after_commit):
            raise CoreError(
                "Operation plan uses after_commit middlewares but tx() is not enabled",
            )

        if any(self.specs(stage) for stage in Stage.iter_tx()):
            raise CoreError(
                "Operation plan uses tx_* or after_commit middlewares but tx() is not enabled",
            )

    # ....................... #

    def build(self, stage: Stage) -> tuple[MiddlewareSpec, ...]:
        deduped = _dedupe_specs(self.specs(stage), stage_label=stage.value)
        return _sort_by_priority(deduped)

    # ....................... #

    def specs_for_chain(self, stage: Stage) -> tuple[MiddlewareSpec, ...]:
        return self.build(stage)

    # ....................... #

    @classmethod
    def merge(
        cls,
        *plans: Self,
        allow_later_route_override: bool = False,
    ) -> Self:
        merged_specs: dict[Stage, tuple[MiddlewareSpec, ...]] = {}
        tx_route: str | None = None

        for plan in plans:
            if plan.tx_route is not None:
                if tx_route is None:
                    tx_route = plan.tx_route
                elif tx_route != plan.tx_route:
                    if allow_later_route_override:
                        tx_route = plan.tx_route
                    else:
                        raise CoreError(
                            "Conflicting transaction routes for one operation: "
                            f"{tx_route!r} vs {plan.tx_route!r}",
                        )

            for stage in Stage.iter_all():
                specs = plan.specs(stage)
                if not specs:
                    continue

                merged_specs[stage] = _merge_stage_tuple(
                    merged_specs.get(stage, ()),
                    specs,
                )

        return cls(tx_route=tx_route, stage_specs=merged_specs)

    # ....................... #

    @classmethod
    def merge_base_and_specific(cls, base: Self, specific: Self) -> Self:
        return cls.merge(
            base,
            specific,
            allow_later_route_override=True,
        )
