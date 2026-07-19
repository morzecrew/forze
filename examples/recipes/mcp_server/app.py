"""Expose a mock Forze aggregate over MCP, for poking with the MCP Inspector.

A tiny in-process ``forze_mock`` "Notes" document aggregate is wired into a `FastMCP` server
via ``forze_mcp`` — every document operation becomes an MCP tool that runs through the normal
Forze pipeline. This is the inbound (driving-adapter) half of the AI integration: an MCP
client calls a tool, Forze validates the input DTO, runs the operation, and returns the
result. No Docker, no real database — the document store is an in-memory ``MockState``.

Run it (Streamable HTTP on http://127.0.0.1:8000/mcp), then point the Inspector at it::

    uv run python -m examples.recipes.mcp_server.app
    npx -y @modelcontextprotocol/inspector            # choose "Streamable HTTP", URL above

A couple of notes are seeded at startup, so ``notes.list`` / ``notes.get`` return data
immediately; ``include_writes=True`` also exposes ``notes.create`` / ``update`` / ``kill`` so
you can mutate state and read it back. The server also registers the querying-DSL guidance
prompts (``forze.querying`` / ``forze.aggregates``, in the Inspector's "Prompts" tab) that
teach how to build ``notes.list`` filters/sorts/pagination, and a ``notes://{id}`` resource
template (in "Resources") to fetch a single note by id. (For a stdio Inspector session
instead, change the transport at the bottom to ``"stdio"`` and launch this module as the
Inspector command.)

Logging is routed through Forze's structured logger: ``LoggingMiddleware`` emits an access
line per MCP message (method, tool/resource target, duration, outcome), and uvicorn's own
loggers are reattached so its startup/HTTP lines share that format instead of the default
``INFO:     ...`` plaintext.

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
from forze.base.logging import (
    AccessLogSampler,
    attach_foreign_loggers,
    configure_logging,
)
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.aggregates.document.factories import build_document_registry
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_mcp import (
    LoggingMiddleware,
    ResourceTemplateSpec,
    build_mcp_server,
    register_dsl_query_prompts,
    register_resource_templates,
    register_schema_resources,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #
# Domain — a minimal Notes aggregate.


# --8<-- [start:aggregate]
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
NS = SPEC.default_namespace
# --8<-- [end:aggregate]

# ----------------------- #


def build_registry() -> FrozenOperationRegistry:
    """Build the (frozen) Notes operation registry."""

    return build_document_registry(SPEC).freeze()


def build_context_factory() -> tuple[ExecutionContextFactory, MockState]:
    """Build a context factory backed by a single shared in-memory context.

    One :class:`ExecutionContext` is created and reused for every tool call (the factory
    just hands back that same instance), so resolved operations and ports stay memoized
    across calls instead of being rebuilt each time — and the one ``MockState`` behind it
    makes a write from one call visible to the next. This mock has no lifecycle steps, so
    a bare shared context is enough; a production server with real adapters should instead
    drive ``ctx_factory`` and ``lifespan`` from one ``ExecutionRuntime`` (pass
    ``runtime.get_context`` here and ``forze_mcp.runtime_lifespan(runtime)`` to the
    server), which additionally runs lifecycle startup/shutdown.
    """

    state = MockState()
    frozen = DepsRegistry.from_modules(MockDepsModule(state=state)).freeze().resolve()
    ctx = ExecutionContext(deps=frozen)

    return (lambda: ctx), state


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

    Also attaches the querying-DSL guidance prompts (Inspector's "Prompts" tab:
    ``forze.querying`` / ``forze.aggregates``) and a field-schema resource
    (``schema://notes``, in "Resources") so an agent can discover which Note fields are
    filterable/sortable and how to build a ``notes.list`` query.
    """

    # --8<-- [start:server]
    server = build_mcp_server(
        registry,
        ctx_factory,
        name="forze-notes",
        include_writes=True,  # demo: expose create/update/kill too, not just reads
    )
    # --8<-- [end:server]
    register_dsl_query_prompts(server)
    register_schema_resources(server, SPEC)
    # Expose get-by-id as a resource template: read `notes://<uuid>` to fetch one note
    # (runs the GET operation through the same governed pipeline as the tools).
    register_resource_templates(
        server,
        registry,
        ctx_factory,
        [ResourceTemplateSpec(op=NS.key(DocumentKernelOp.GET), scheme="notes")],
    )
    # Structured access log per MCP message (method, tool/resource target, duration, outcome).
    # Full mode here so the demo shows a line for every message; production defaults to
    # sampled (errors always, successes 1-in-N).
    server.add_middleware(LoggingMiddleware(access_log=AccessLogSampler(mode="full")))

    return server


# ----------------------- #


def main() -> None:
    # Route everything through Forze's structured logging. ``configure_logging`` sets up our
    # own logs (incl. the LoggingMiddleware access lines); ``attach_foreign_loggers`` takes
    # over the loggers owned by FastMCP (``fastmcp.*`` — configured at import) and uvicorn so
    # their lines share the same format instead of the default ``INFO:     ...`` plaintext.
    configure_logging(render_mode="console")
    attach_foreign_loggers(
        ["fastmcp", "uvicorn", "uvicorn.error", "uvicorn.access"],
        render_mode="console",
    )

    registry = build_registry()
    ctx_factory, _ = build_context_factory()

    # Seed in a throwaway loop, then hand the server its own loop via run().
    asyncio.run(seed(registry, ctx_factory))

    server = build_server(registry, ctx_factory)
    # ``log_config=None`` stops uvicorn from running its own ``dictConfig`` on startup, which
    # would otherwise reset the handlers ``attach_foreign_loggers`` just installed.
    server.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=8000,
        show_banner=False,  # the (forze-formatted) "Starting MCP server" line already has the URL
        uvicorn_config={"log_config": None},
    )


if __name__ == "__main__":
    main()
