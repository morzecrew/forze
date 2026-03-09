"""Unit tests for mock dependency wiring."""

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.pubsub import PubSubPublishDepKey, PubSubSpec
from forze.application.contracts.queue import QueueReadDepKey, QueueSpec, QueueWriteDepKey
from forze.application.contracts.search import SearchFieldSpec, SearchIndexSpec, SearchSpec
from forze.application.contracts.stream import (
    StreamGroupDepKey,
    StreamWriteDepKey,
)
from forze.application.contracts.stream.specs import StreamSpec
from forze.application.execution import ExecutionContext
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_mock.execution import MockStateDepKey

# ----------------------- #


class _Doc(Document, SoftDeletionMixin):
    title: str


class _Create(CreateDocumentCmd):
    title: str


class _Update(BaseDTO):
    title: str | None = None


class _Read(ReadDocument):
    title: str
    is_deleted: bool = False


class _Msg(BaseModel):
    value: str


def _doc_spec() -> DocumentSpec[_Read, _Doc, _Create, _Update]:
    return DocumentSpec(
        namespace="items",
        read={"source": "items", "model": _Read},
        write={
            "source": "items",
            "models": {
                "domain": _Doc,
                "create_cmd": _Create,
                "update_cmd": _Update,
            },
        },
    )


def _search_spec() -> SearchSpec[_Read]:
    return SearchSpec(
        namespace="items",
        model=_Read,
        indexes={"main": SearchIndexSpec(fields=[SearchFieldSpec(path="title")])},
        default_index="main",
    )


async def test_mock_deps_module_registers_expected_contracts() -> None:
    deps = MockDepsModule()()
    assert deps.exists(MockStateDepKey)
    assert deps.exists(PubSubPublishDepKey)
    assert deps.exists(QueueReadDepKey)
    assert deps.exists(StreamGroupDepKey)


async def test_execution_context_can_use_mock_document_and_search() -> None:
    ctx = ExecutionContext(deps=MockDepsModule()())
    spec = _doc_spec()
    doc = ctx.doc_write(spec)
    created = await doc.create(_Create(title="Hello"))

    found = await ctx.doc_read(spec).get(created.id)
    assert found.id == created.id

    search_hits, count = await ctx.search(_search_spec()).search("hello")
    assert count == 1
    assert search_hits[0].id == created.id


async def test_execution_context_resolves_optional_contract_ports() -> None:
    ctx = ExecutionContext(deps=MockDepsModule()())

    queue_read = ctx.dep(QueueReadDepKey)(ctx, QueueSpec(namespace="q", model=_Msg))
    queue_write = ctx.dep(QueueWriteDepKey)(ctx, QueueSpec(namespace="q", model=_Msg))
    pubsub = ctx.dep(PubSubPublishDepKey)(ctx, PubSubSpec(namespace="p", model=_Msg))
    stream_write = ctx.dep(StreamWriteDepKey)(ctx, StreamSpec(namespace="s", model=_Msg))
    stream_group = ctx.dep(StreamGroupDepKey)(ctx, StreamSpec(namespace="s", model=_Msg))

    msg_id = await queue_write.enqueue("tasks", _Msg(value="x"))
    received = await queue_read.receive("tasks")
    assert received[0]["id"] == msg_id

    await pubsub.publish("topic", _Msg(value="ping"))

    stream_id = await stream_write.append("events", _Msg(value="a"))
    rows = await stream_group.read("g", "c", {"events": "0"})
    assert rows[0]["id"] == stream_id
    assert rows[0]["payload"].value == "a"
