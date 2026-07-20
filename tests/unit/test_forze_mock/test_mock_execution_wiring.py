"""Smoke test :class:`~forze_mock.execution.MockDepsModule` factory wiring."""

from datetime import timedelta

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.dlock import (
    DistributedLockQueryDepKey,
    DistributedLockSpec,
)
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionEventSpec,
    DurableFunctionStepDepKey,
)
from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandDepKey,
    DurableWorkflowInvokeSpec,
    DurableWorkflowQueryDepKey,
    DurableWorkflowScheduleCommandDepKey,
    DurableWorkflowScheduleQueryDepKey,
    DurableWorkflowSpec,
)
from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchCommandDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.contracts.secrets import SecretsDepKey
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.execution import MockDepsModule, MockStateDepKey
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _D(Document):
    n: str


class _C(CreateDocumentCmd):
    n: str


class _U(BaseDTO):
    n: str | None = None


class _R(ReadDocument):
    n: str


class _S(BaseModel):
    id: str
    t: str


def test_mock_deps_module_resolves_shared_state_and_core_ports() -> None:
    mod = MockDepsModule()
    ctx = context_from_deps(mod())

    assert ctx.deps.provide(MockStateDepKey) is mod.state

    dspec = DocumentSpec(
        name="d",
        read=_R,
        write=DocumentWriteTypes(domain=_D, create_cmd=_C, update_cmd=_U),
    )
    q_adapter = ctx.document.query(dspec)
    c_adapter = ctx.document.command(dspec)
    assert q_adapter.__class__ is c_adapter.__class__

    search = SearchSpec(name="s", model_type=_S, fields=["t"])
    assert ctx.search.query(search) is not None

    class _P(BaseModel):
        n: str = ""

    aspec = AnalyticsSpec(
        name="a",
        read=_S,
        queries={"q": AnalyticsQueryDefinition(params=_P)},
    )
    assert ctx.analytics.query(aspec) is not None

    assert ctx.cache(CacheSpec(name="cache1")) is not None

    assert ctx.deps.provide(SecretsDepKey) is not None
    assert ctx.deps.provide(DistributedLockQueryDepKey)(
        ctx, DistributedLockSpec(name="lk", ttl=timedelta(seconds=5))
    ) is not None
    assert ctx.deps.provide(SearchCommandDepKey)(ctx, search) is not None
    assert ctx.deps.provide(SearchResultSnapshotDepKey)(
        ctx, SearchResultSnapshotSpec(name="snap"),
    ) is not None
    assert ctx.deps.provide(EmbeddingsProviderDepKey)(
        ctx, EmbeddingsSpec(name="emb", dimensions=4),
    ) is not None

    wf_in = _C
    wf_out = _R
    wf_spec = DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=wf_in, return_type=wf_out),
    )
    assert ctx.deps.provide(DurableWorkflowCommandDepKey)(ctx, wf_spec) is not None
    assert ctx.deps.provide(DurableWorkflowQueryDepKey)(ctx, wf_spec) is not None
    assert ctx.deps.provide(DurableWorkflowScheduleCommandDepKey)(ctx, wf_spec) is not None
    assert ctx.deps.provide(DurableWorkflowScheduleQueryDepKey)(ctx, wf_spec) is not None

    evt_spec = DurableFunctionEventSpec(
        name="evt",
        codec=PydanticModelCodec(model_type=_S),
    )
    assert ctx.deps.provide(DurableFunctionEventCommandDepKey)(ctx, evt_spec) is not None
    assert ctx.deps.provide(DurableFunctionStepDepKey) is not None

    leg_a = SearchSpec(name="a", model_type=_S, fields=["t"])
    leg_b = SearchSpec(name="b", model_type=_S, fields=["t"])
    hub = HubSearchSpec(name="hub", model_type=_S, members=[leg_a])
    assert ctx.deps.provide(HubSearchQueryDepKey)(ctx, hub) is not None
    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    assert ctx.deps.provide(FederatedSearchQueryDepKey)(ctx, fed) is not None
