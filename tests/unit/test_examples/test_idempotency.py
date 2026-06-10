"""Idempotency recipe — a replayed create dedupes to the first result (mock store, no Docker)."""

from __future__ import annotations

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_mock import MockDepsModule

from examples.recipes.idempotency.app import idempotent_create


async def test_idempotent_create_dedupes() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        first, second = await idempotent_create(runtime.get_context())
    assert first.id == second.id
    assert first.item == "widget"
