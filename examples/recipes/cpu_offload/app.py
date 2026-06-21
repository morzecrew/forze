"""Recipe: offload heavy synchronous work off the event loop in a two-phase prepare.

A bulk-import handler parses and validates a raw blob — synchronous, CPU-bound work
that on the event loop would block *every* other task for its whole duration. ``run_cpu``
moves it to a bounded worker pool inside ``prepare`` (which runs **outside** the
transaction), so the loop stays responsive and no connection is held during the parse;
``apply`` then writes the result **inside** the transaction.

Why not bare ``asyncio.to_thread``? ``run_cpu`` is a context-bound seam: it honors the
invocation deadline, carries tracing/tenant context into the worker, and — crucially —
runs **inline and deterministically under simulation**, so this handler is DST-testable.
A raw ``to_thread`` raises ``RealIOForbidden`` under the simulator.

Run it:  uv run python -m examples.recipes.cpu_offload.app   (no infra — mock store)
Exercised by tests/unit/test_examples/test_cpu_offload.py.
"""

from __future__ import annotations

import asyncio

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import run_cpu
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import (
    TwoPhaseDocumentBuilder,
    TwoPhaseDocumentHandler,
)
from forze_mock import MockDepsModule

# --8<-- [start:domain]
class Article(Document):
    title: str
    word_count: int


class CreateArticle(CreateDocumentCmd):
    title: str
    word_count: int


class ReadArticle(ReadDocument):
    title: str
    word_count: int


class ImportRequest(BaseDTO):
    raw: str  # a title line followed by the body
# --8<-- [end:domain]


ARTICLE_SPEC = DocumentSpec(
    name="articles",
    read=ReadArticle,
    write={"domain": Article, "create_cmd": CreateArticle},
)


# --8<-- [start:parse]
def parse_article(raw: str) -> CreateArticle:
    """Heavy *synchronous* parse + validation — stands in for a large blob run through
    pydantic. On the event loop this blocks every other task for its whole duration."""

    title, _, body = raw.partition("\n")

    return CreateArticle(title=title.strip(), word_count=len(body.split()))
# --8<-- [end:parse]


# --8<-- [start:handler]
@attrs.define(slots=True, kw_only=True, frozen=True)
class ImportArticle(
    TwoPhaseDocumentHandler[ImportRequest, CreateArticle, ReadArticle, CreateArticle]
):
    """Parse off the loop (outside the tx), then write the parsed article (inside it)."""

    async def prepare(self, args: ImportRequest) -> CreateArticle:
        # OUTSIDE the transaction, OFF the event loop: the parse cannot stall the runtime
        # and holds no connection. Inline + deterministic under simulation.
        return await run_cpu(parse_article, args.raw)

    async def apply(self, args: ImportRequest, payload: CreateArticle) -> ReadArticle:
        # INSIDE the transaction — only the write is wrapped.
        return await self.writer.create(payload)
# --8<-- [end:handler]


# --8<-- [start:registry]
IMPORT_ARTICLE = "articles.import"

REGISTRY = (
    OperationRegistry(
        handlers={
            IMPORT_ARTICLE: TwoPhaseDocumentBuilder(
                spec=ARTICLE_SPEC,
                build=lambda reader, writer: ImportArticle(reader=reader, writer=writer),
            )
        }
    )
    .bind(IMPORT_ARTICLE)
    .two_phase()
    .bind_tx()
    .set_route("mock")
    .finish(deep=True)
    .freeze()
)
# --8<-- [end:registry]


# --8<-- [start:scenario]
async def import_article(ctx: ExecutionContext) -> ReadArticle:
    created = await REGISTRY.resolve(IMPORT_ARTICLE, ctx)(
        ImportRequest(raw="Widgets 101\nthe quick brown fox jumps over it")
    )

    # The write committed and is readable afterwards.
    stored = await ctx.document.query(ARTICLE_SPEC).get(created.id)
    assert stored is not None and stored.word_count == created.word_count

    return created
# --8<-- [end:scenario]


async def main() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        article = await import_article(runtime.get_context())
        print(f"imported: title={article.title!r} word_count={article.word_count}")


if __name__ == "__main__":
    asyncio.run(main())
