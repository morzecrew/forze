"""A question-answering agent: governed operations as its tools, a declared egress for its model.

Runs in-process on the mock. Three things are worth separating when you read it:

* The **tools** are the notes aggregate's own list and aggregate operations, projected into
  a read-only palette. The agent cannot write, because no write operation is in the palette.
* The **model call** is an ordinary outbound HTTP service, declared `egress_sensitive` and
  acknowledged, so the hop that carries the question and the retrieved rows out of the
  trust boundary is a reviewed wiring fact.
* The **loop** is this file's own code. The framework projects tools and dispatches one
  call; deciding to take another turn is the application's business.

The provider is answered here by a registered handler rather than a network call, so the
example is deterministic and needs no API key. The handler is the only stub: the palette
it chooses from, the dispatch it triggers and the governance around both are real.

Run it: `uv run python -m examples.recipes.query_agent.app`
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.application.contracts.querying import QueryFieldPolicy
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.application.execution.deps import DepsRegistry
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_kits.aggregates.document import DocumentKernelOp, build_document_registry
from forze_kits.integrations.agent_tools import (
    OperationToolset,
    ToolResult,
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)
from forze_mock import MockDepsModule, MockHttpRegistry

# ----------------------- #


# --8<-- [start:aggregate]
class Note(Document):
    title: str
    category: str
    body: str


class NoteRead(ReadDocument):
    title: str
    category: str
    body: str


class CreateNote(CreateDocumentCmd):
    title: str
    category: str
    body: str


NOTES = StrKeyNamespace(prefix="notes")

notes_spec = DocumentSpec(
    name="notes",
    read=NoteRead,
    write={"domain": Note, "create_cmd": CreateNote},
    # The allow-set the agent is told about and held to: `body` is readable but not a
    # field anything may filter, sort or group by.
    query_policy=QueryFieldPolicy(
        filterable={"title", "category"},
        sortable={"title"},
        aggregatable={"category"},
    ),
)


def notes_registry() -> FrozenOperationRegistry:
    return build_document_registry(notes_spec, ns=NOTES).freeze()


# --8<-- [end:aggregate]


# --8<-- [start:palette]
def answering_tools(registry: FrozenOperationRegistry) -> OperationToolset:
    """The agent's whole world: list notes, and count them by category."""

    return operation_tools(
        registry,
        include=[
            NOTES.key(DocumentKernelOp.LIST),
            NOTES.key(DocumentKernelOp.AGG_LIST),
        ],
    )


# --8<-- [end:palette]


# --8<-- [start:egress]
class ModelArgs(BaseModel):
    question: str
    tools: list[dict[str, Any]]


class ModelToolCall(BaseModel):
    id: str
    name: str
    input: dict[str, Any]


class ModelReply(BaseModel):
    text: str = ""
    tool_call: ModelToolCall | None = None


model_service = HttpServiceSpec(
    name="model",
    operations={
        "messages": HttpOperationSpec(
            name="messages",
            method="POST",
            path="/v1/messages",
            args_type=ModelArgs,
            return_type=ModelReply,
        ),
    },
)


def model_wiring() -> dict[str, HttpServiceConfig]:
    """How the provider hop is wired in a deployment that really calls one.

    The question and the rows retrieved for it leave the trust boundary on this route, so
    it says so — and saying so requires accepting it. Declare the first without the second
    and wiring fails closed with `http_egress_unacknowledged` rather than shipping the
    data and mentioning it in a comment.
    """

    return {
        "model": HttpServiceConfig(
            base_url="https://api.provider.example",
            egress_sensitive=True,
            acknowledge_data_egress=True,
        )
    }


# --8<-- [end:egress]


# --8<-- [start:loop]
async def answer(question: str, *, ctx: ExecutionContext, registry: FrozenOperationRegistry) -> str:
    """One turn: ask the model, run the tool it picked, hand back what came out.

    A production loop keeps going — feeding the result back, letting the model ask for
    another tool, stopping when it answers in prose. The shape does not change: the
    palette is projected once, and every call the model makes returns here.
    """

    tools = answering_tools(registry)
    reply = await ctx.http.service(model_service).invoke(
        "messages",
        ModelArgs(
            question=question,
            tools=[
                {
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                }
                for tool in tools.defs
            ],
        ),
    )

    if reply.tool_call is None:
        return reply.text

    result = await dispatch_tool_use(
        ToolUse(
            id=reply.tool_call.id,
            name=reply.tool_call.name,
            input=reply.tool_call.input,
        ),
        ctx=ctx,
        tools=tools,
    )

    return _render(result)


def _render(result: ToolResult) -> str:
    """What the next turn would be told — the rows, or the error the model can act on."""

    if result.is_error:
        return f"tool refused: {result.content}"

    hits = result.content["hits"] if isinstance(result.content, dict) else []

    return "; ".join(str(hit) for hit in hits)


# --8<-- [end:loop]


# --8<-- [start:stub]
def canned_model(args: ModelArgs | None) -> ModelReply:
    """Stands in for the provider: picks a tool from the palette it was given.

    A real model reads the descriptions — which carry the filterable fields and their
    operators, so it does not have to guess — and answers with a tool call in its own
    format, which the loop adapts to a `ToolUse`. This one keyword-matches, so the example
    is deterministic.
    """

    question = args.question if args is not None else ""
    names = [tool["name"] for tool in (args.tools if args is not None else [])]

    if "how many" in question.lower():
        return ModelReply(
            tool_call=ModelToolCall(
                id="call-1",
                name=next(name for name in names if name.endswith(".agg_list")),
                input={
                    "aggregates": {
                        "$groups": {"category": "category"},
                        "$computed": {"n": {"$count": None}},
                    }
                },
            )
        )

    return ModelReply(
        tool_call=ModelToolCall(
            id="call-2",
            name=next(name for name in names if name.endswith(".list")),
            input={"filters": {"$values": {"category": "postgres"}}, "sorts": {"title": "asc"}},
        )
    )


# --8<-- [end:stub]


SEED: tuple[tuple[str, str, str], ...] = (
    ("index bloat", "postgres", "reindex concurrently"),
    ("vacuum settings", "postgres", "autovacuum thresholds"),
    ("stream trimming", "redis", "XTRIM MINID"),
)


async def seed(ctx: ExecutionContext, registry: FrozenOperationRegistry) -> None:
    for title, category, body in SEED:
        await run_operation(
            registry,
            NOTES.key(DocumentKernelOp.CREATE),
            CreateNote(title=title, category=category, body=body),
            ctx,
        )


async def run() -> tuple[str, str]:
    """Seed three notes, then ask two questions that need two different tools."""

    module = MockDepsModule(http=MockHttpRegistry().on("model", "messages", canned_model))
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    registry = notes_registry()

    async with runtime.scope():
        ctx = runtime.get_context()
        await seed(ctx, registry)

        return (
            await answer("what do we know about postgres?", ctx=ctx, registry=registry),
            await answer("how many notes per category?", ctx=ctx, registry=registry),
        )


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


async def main() -> None:
    _setup_logging()
    listed, counted = await run()

    logging.info("listed:  %s", listed)
    logging.info("counted: %s", counted)


if __name__ == "__main__":
    asyncio.run(main())
