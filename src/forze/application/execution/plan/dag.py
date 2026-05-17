"""Plan DAG types and compilation helpers for schedulable execution stages."""

from __future__ import annotations

import re
from collections import defaultdict
from functools import cached_property
from typing import Callable, Mapping, Sequence

import attrs

from forze.base.errors import CoreError

from .spec import MiddlewareSpec, frozenset_capability_keys

# ----------------------- #

_DAG_LABEL_RE = re.compile(r"^[\w.-]+$")

type DagSpecFactory[F] = Callable[
    [F, int, frozenset[str], frozenset[str], str | None],
    MiddlewareSpec,
]


def dag_completion_key(scope: str, node_id: str) -> str:
    """Return the synthetic capability key used to mark one DAG node as complete."""

    return f"dag.{scope}.{node_id}"


def _assert_acyclic(preds: Mapping[str, frozenset[str]]) -> None:
    node_ids = frozenset(preds.keys())
    indeg = {node_id: len(preds[node_id]) for node_id in node_ids}
    succs: dict[str, list[str]] = defaultdict(list)

    for consumer, producers in preds.items():
        for producer in producers:
            succs[producer].append(consumer)

    for adj in succs.values():
        adj.sort()

    ready = sorted(node_id for node_id in node_ids if indeg[node_id] == 0)
    removed = 0

    while ready:
        current = ready.pop(0)
        removed += 1

        for nxt in succs[current]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                ready.append(nxt)
                ready.sort()

    if removed != len(node_ids):
        raise CoreError("Plan DAG contains a directed cycle")


def _assert_extra_provides_do_not_shadow_completion_keys[F](
    scope: str,
    nodes: Sequence[DagNode[F]],
) -> None:
    completion_keys = {node.id: dag_completion_key(scope, node.id) for node in nodes}
    reserved = frozenset(completion_keys.values())

    for node in nodes:
        mine = completion_keys[node.id]
        shadowing = (node.provides & reserved) - {mine}

        if shadowing:
            raise CoreError(
                f"Plan DAG node {node.id!r} (scope {scope!r}) declares "
                f"provides overlapping reserved completion keys {sorted(shadowing)!r}",
            )


@attrs.define(slots=True, kw_only=True, frozen=True)
class DagNode[F]:
    """One user-facing node in a stage DAG."""

    id: str
    factory: F
    priority: int = attrs.field(
        default=0,
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ],
    )
    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    label: str | None = None


@attrs.define(slots=True, kw_only=True, frozen=True)
class PlanDag[F]:
    """Explicit DAG for one schedulable stage."""

    nodes: Sequence[DagNode[F]] = attrs.field(factory=tuple)
    edges: Sequence[tuple[str, str]] = attrs.field(factory=tuple)

    def _validate_label(self, kind: str, value: str) -> None:
        if not value:
            raise CoreError(f"Plan DAG {kind} cannot be empty")

        if not _DAG_LABEL_RE.fullmatch(value):
            raise CoreError(
                f"Plan DAG {kind} {value!r} must match "
                r"^[\w.-]+$ (letters, digits, underscore, hyphen, dot)",
            )

    def _index_nodes(self) -> dict[str, DagNode[F]]:
        by_id: dict[str, DagNode[F]] = {}

        for node in self.nodes:
            self._validate_label("node id", node.id)

            if node.id in by_id:
                raise CoreError(f"Duplicate plan DAG node id {node.id!r}")

            by_id[node.id] = node

        return by_id

    def _build_graph(self) -> dict[str, frozenset[str]]:
        preds: dict[str, set[str]] = {node_id: set() for node_id in self._node_ids}
        seen: set[tuple[str, str]] = set()

        for producer, consumer in self.edges:
            if producer == consumer:
                raise CoreError(
                    f"Plan DAG edge ({producer!r}, {consumer!r}) cannot be a self-loop",
                )

            if producer not in self._node_ids:
                raise CoreError(
                    f"Unknown plan DAG node id {producer!r} in edge "
                    f"({producer!r}, {consumer!r})",
                )

            if consumer not in self._node_ids:
                raise CoreError(
                    f"Unknown plan DAG node id {consumer!r} in edge "
                    f"({producer!r}, {consumer!r})",
                )

            if (producer, consumer) in seen:
                continue

            seen.add((producer, consumer))
            preds[consumer].add(producer)

        return {node_id: frozenset(preds[node_id]) for node_id in self._node_ids}

    @cached_property
    def _node_ids(self) -> frozenset[str]:
        by_id = self._index_nodes()
        return frozenset(by_id.keys())

    def compile_(
        self,
        *,
        scope: str,
        spec_factory: DagSpecFactory[F],
    ) -> tuple[MiddlewareSpec, ...]:
        self._validate_label("scope", scope)

        if not self.nodes:
            return ()

        preds = self._build_graph()
        _assert_acyclic(preds)
        _assert_extra_provides_do_not_shadow_completion_keys(scope, self.nodes)

        specs: list[MiddlewareSpec] = []

        for node in self.nodes:
            completion = dag_completion_key(scope, node.id)
            dag_requires = frozenset(
                dag_completion_key(scope, producer_id)
                for producer_id in preds[node.id]
            )
            specs.append(
                spec_factory(
                    node.factory,
                    node.priority,
                    dag_requires | node.requires,
                    frozenset({completion}) | node.provides,
                    node.label if node.label is not None else node.id,
                )
            )

        return tuple(specs)

__all__ = [
    "DagNode",
    "PlanDag",
    "dag_completion_key",
]
