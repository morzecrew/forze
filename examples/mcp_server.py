"""Expose a mock Forze aggregate over MCP, for poking with the MCP Inspector.

A tiny in-process ``forze_mock`` "Notes" document aggregate is wired into a `FastMCP` server
via ``forze_mcp`` — every document operation becomes an MCP tool that runs through the normal
Forze pipeline. This is the inbound (driving-adapter) half of the AI integration: an MCP
client calls a tool, Forze validates the input DTO, runs the operation, and returns the
result. No Docker, no real database — the document store is an in-memory ``MockState``.

Run it (Streamable HTTP on http://127.0.0.1:8000/mcp), then point the Inspector at it::

    uv run python -m examples.mcp_server
    npx -y @modelcontextprotocol/inspector            # choose "Streamable HTTP", URL above

A couple of notes are seeded at startup, so ``notes.list`` / ``notes.get`` return data
immediately; ``include_writes=True`` also exposes ``notes.create`` / ``update`` / ``kill`` so
you can mutate state and read it back. The server also registers the querying-DSL guidance
prompts (``forze.querying`` / ``forze.aggregates``, in the Inspector's "Prompts" tab) that
teach how to build ``notes.list`` filters/sorts/pagination. (For a stdio Inspector session
instead, change the transport at the bottom to ``"stdio"`` and launch this module as the
Inspector command.)

It is also executed by ``tests/unit/test_examples/test_mcp_server_example.py`` — the example
is the spec, and the test proves operations are preserved as tools and round-trip over MCP.
"""

from __future__ import annotations

import asyncio

from fastmcp import FastMCP

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.deps import DepsRegistry
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.aggregates.document.factories import build_document_registry
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.document.value_objects import DocumentDTOs
from forze_mcp import build_mcp_server, register_dsl_query_prompts
from forze_mock import MockDepsModule, MockState

# ----------------------- #
# Domain — a minimal Notes aggregate.


class Note(Document):
    title: str = ""
    body: str = ""


class NoteRead(ReadDocument):
    title: str
    body: str


class NoteInput(BaseDTO):
    """Create payload — only the domain fields a caller supplies. The server assigns the
    id and stamps timestamps, so identity never appears in the payload (or the tool schema).
    """

    title: str = ""
    body: str = ""


class NoteUpdate(BaseDTO):
    title: str | None = None
    body: str | None = None


SPEC = DocumentSpec(
    name="notes",
    read=NoteRead,
    write=DocumentWriteTypes(domain=Note, create_cmd=NoteInput, update_cmd=NoteUpdate),
)
DTOS = DocumentDTOs(read=NoteRead, create=NoteInput, update=NoteUpdate)
NS = SPEC.default_namespace

# ----------------------- #


def build_registry() -> FrozenOperationRegistry:
    """Build the (frozen) Notes operation registry."""

    return build_document_registry(SPEC, DTOS).freeze()


def build_context_factory() -> tuple[ExecutionContextFactory, MockState]:
    """Build a context factory backed by a shared in-memory ``MockState``.

    The same ``MockState`` backs every context, so data written by one tool call is visible
    to the next — exactly what you want when poking at the server interactively.
    """

    state = MockState()
    frozen = DepsRegistry.from_modules(MockDepsModule(state=state)).freeze().resolve()

    return (lambda: ExecutionContext(deps=frozen)), state


async def seed(
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
) -> None:
    """Create a couple of notes so reads return data immediately."""

    for title, body in (("welcome", "first note"), ("todo", "try the inspector")):
        await run_operation(
            registry,
            NS.key(DocumentKernelOp.CREATE),
            NoteInput(title=title, body=body),
            ctx_factory(),
        )


def build_server(
    registry: FrozenOperationRegistry,
    ctx_factory: ExecutionContextFactory,
) -> FastMCP:
    """Build a FastMCP server exposing every Notes operation as a tool.

    Also attaches the querying-DSL guidance prompts, so the Inspector's "Prompts" tab shows
    ``forze.querying`` / ``forze.aggregates`` — pull them to learn how to build ``notes.list``
    filters/sorts/pagination.
    """

    server = build_mcp_server(
        registry,
        ctx_factory,
        name="forze-notes",
        include_writes=True,  # demo: expose create/update/kill too, not just reads
    )
    register_dsl_query_prompts(server)

    return server


# ----------------------- #


def main() -> None:
    registry = build_registry()
    ctx_factory, _ = build_context_factory()

    # Seed in a throwaway loop, then hand the server its own loop via run().
    asyncio.run(seed(registry, ctx_factory))

    server = build_server(registry, ctx_factory)
    server.run(transport="streamable-http", host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
