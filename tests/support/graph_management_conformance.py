"""Shared ``GraphManagementPort`` conformance battery.

The port promises idempotent schema provisioning — "create the module's constraints/indexes
if absent (idempotent)" — and on Neo4j that provisioning is *load-bearing*: without the
uniqueness constraints, Cypher ``CREATE`` writes a second node under a key that is supposed
to identify one, so ``ensure_schema()`` is a correctness requirement rather than a
performance tweak.

That made the port's absence from the in-memory adapter a real gap rather than a cosmetic
one: the one line every graph application is told to run at startup could not be resolved
against the oracle at all, so the provisioning step was the step no unit test or simulation
could execute. This battery runs the same promises against both.

What each check pins:

1. Provisioning is resolvable and succeeds — the startup path exists on every backend.
2. Provisioning twice is not an error, so it is safe at every boot.
3. Teardown is idempotent, and safe before anything was ever provisioned.
4. Provisioning after teardown restores the schema — the cycle a test suite runs.
5. A provisioned module still enforces node-key uniqueness, which is the property the
   whole port exists to establish.

Check 5 is the positive control for the rest: without it, an adapter whose ``ensure_schema``
did nothing at all would pass every idempotency assertion above.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import attrs
import pytest

from forze.application.contracts.graph import GraphManagementPort
from forze.base.exceptions import CoreException

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GraphManagementHarness:
    """One backend's seam for the schema-provisioning battery."""

    management: GraphManagementPort
    """The control-plane port under test."""

    backend: str
    """Label used in assertion messages."""

    create_node: Callable[[str], Any]
    """Create one node under the given key, awaited by the caller.

    A seam rather than a shared call because the command port is built per backend from a
    different spec and client; the battery only needs "make a node with this key".
    """

    unique_key_is_droppable: bool
    """Whether ``drop_schema`` actually relaxes node-key uniqueness on this backend.

    ``False`` for the in-memory adapter, whose store is keyed by ``(kind, key)`` — the
    constraint is a property of the data structure and cannot be removed. Declared rather
    than worked around: modelling a droppable constraint would mean letting the oracle
    accept duplicate keys, which is the permissive direction that hides real bugs.
    """


Check = Callable[[GraphManagementHarness], Any]
"""One battery check."""


# ....................... #


async def check_provisioning_succeeds(h: GraphManagementHarness) -> None:
    """The startup step resolves and runs on every backend."""

    await h.management.ensure_schema()


async def check_provisioning_is_idempotent(h: GraphManagementHarness) -> None:
    """Provisioning twice reconciles rather than failing — safe at every boot."""

    await h.management.ensure_schema()
    await h.management.ensure_schema()


async def check_teardown_is_idempotent(h: GraphManagementHarness) -> None:
    """Teardown twice, and teardown of a module nothing provisioned, both succeed.

    The unprovisioned case is the one that bites: a test fixture or a reset routine runs
    teardown first, before anything has created the schema.
    """

    await h.management.drop_schema()
    await h.management.drop_schema()

    await h.management.ensure_schema()
    await h.management.drop_schema()
    await h.management.drop_schema()


async def check_provisioning_after_teardown_restores_the_schema(
    h: GraphManagementHarness,
) -> None:
    """The provision/teardown/provision cycle a test suite runs between cases."""

    await h.management.ensure_schema()
    await h.management.drop_schema()
    await h.management.ensure_schema()


async def check_a_provisioned_module_enforces_node_key_uniqueness(
    h: GraphManagementHarness,
) -> None:
    """The property provisioning exists to establish — and this battery's control.

    Without this assertion an ``ensure_schema`` that did nothing whatsoever would satisfy
    every idempotency check above. On Neo4j the constraint is what makes a duplicate key
    fail instead of silently creating a second node; on the in-memory adapter the same
    guarantee comes from the store's shape.
    """

    await h.management.ensure_schema()
    await h.create_node("battery-duplicate-key")

    with pytest.raises(CoreException) as ei:
        await h.create_node("battery-duplicate-key")

    assert ei.value.code == "graph_vertex_conflict", h.backend


# ....................... #

GRAPH_MANAGEMENT_BATTERY: tuple[Check, ...] = (
    check_provisioning_succeeds,
    check_provisioning_is_idempotent,
    check_teardown_is_idempotent,
    check_provisioning_after_teardown_restores_the_schema,
    check_a_provisioned_module_enforces_node_key_uniqueness,
)
