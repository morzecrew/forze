"""CPU-offload recipe — prepare parses off the loop via run_cpu, apply writes."""

from __future__ import annotations

from examples.recipes.cpu_offload.app import import_article
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_mock import MockDepsModule


async def test_import_article_parses_off_loop_then_writes() -> None:
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )
    async with runtime.scope():
        article = await import_article(runtime.get_context())

    assert article.title == "Widgets 101"
    assert article.word_count == 7  # parsed off the event loop by run_cpu
