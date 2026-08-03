"""``MockDepsModule`` composed with real backend modules in one context.

The mock registers everything as a *fallback*: it is a background environment, not a claim
on a key. So "real X, mock everything else" is one deps list, and the keys the real module
covers resolve to it — which is what the plain-vs-routed merge conflict used to forbid.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnDepKey
from forze.application.contracts.crypto import KeyManagementDepKey
from forze.application.contracts.document import (
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.execution import Handler
from forze.application.contracts.inference import InferenceDepKey, InferenceSpec
from forze.application.contracts.tenancy import TenantResolverDepKey
from forze.application.execution import Deps, DepsRegistry, ExecutionContext
from forze.application.execution.operations import check_wiring
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
)
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockInferenceRegistry
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


class _Thing(Document):
    name: str = "x"


class _ThingCreate(CreateDocumentCmd):
    name: str = "x"


class _ThingUpdate(BaseDTO):
    name: str | None = None


class _ThingRead(ReadDocument):
    name: str


_THINGS = DocumentSpec(
    name="things",
    read=_ThingRead,
    write=DocumentWriteTypes(domain=_Thing, create_cmd=_ThingCreate, update_cmd=_ThingUpdate),
)

_LOCAL = InferenceSpec(name="doubler", input=_Features, output=_Score)
_UNCOVERED = InferenceSpec(name="tripler", input=_Features, output=_Score)


class _Doubling:
    def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
        return [_Score(y=item.x * 2.0) for item in instances]


def _local_inference() -> LocalInferenceDepsModule:
    return LocalInferenceDepsModule(
        models={"doubler": LocalInferenceConfig(loader=_Doubling)},
    )


# ....................... #


class TestHybridContext:
    @pytest.mark.asyncio
    async def test_the_real_module_owns_its_routes_and_the_mock_serves_the_rest(self) -> None:
        ctx = context_from_modules(MockDepsModule(), _local_inference())

        assert await ctx.inference.model(_LOCAL).predict(_Features(x=3.0)) == _Score(y=6.0)

        # ...and a plane the real module says nothing about still resolves to the mock.
        created = await ctx.document.command(_THINGS).create(_ThingCreate(name="hybrid"))
        assert created.name == "hybrid"

    @pytest.mark.asyncio
    async def test_module_order_does_not_matter(self) -> None:
        ctx = context_from_modules(_local_inference(), MockDepsModule())

        assert await ctx.inference.model(_LOCAL).predict(_Features(x=1.5)) == _Score(y=3.0)

    @pytest.mark.asyncio
    async def test_a_route_the_real_module_never_declared_falls_back_to_the_mock(self) -> None:
        # The accepted hazard, made concrete: "tripler" is not a local model, so it does
        # not fail — it reaches the mock's inference port, which answers only what the
        # test programmed. Nothing programmed here, so the mock says so explicitly.
        ctx = context_from_modules(
            MockDepsModule(inference=MockInferenceRegistry()),
            _local_inference(),
        )

        with pytest.raises(CoreException) as error:
            await ctx.inference.model(_UNCOVERED).predict(_Features(x=1.0))

        assert error.value.code == "mock.inference.unprogrammed"

    def test_the_report_names_what_the_mock_still_serves(self) -> None:
        ctx = context_from_modules(MockDepsModule(), _local_inference())
        report = ctx.deps.store.fallback_report()

        assert report.hybrid
        # The real routed registration displaced nothing (the mock's is plain, and it
        # still answers every route the local module does not declare) — it is *served*,
        # and the report is where a reader sees that.
        assert InferenceDepKey.name in report.served_names()
        assert InferenceDepKey in report.served_plain
        assert ctx.deps.store.routed_deps[InferenceDepKey].keys() == {"doubler"}

    def test_the_report_separates_the_catch_all_hazard_from_a_plain_mocked_plane(self) -> None:
        # The two are not the same risk and must not read the same: inference is real for
        # its own routes with the mock behind it (a typo there resolves silently), while
        # documents are simply not wired for real at all.
        report = context_from_modules(MockDepsModule(), _local_inference()).deps.store
        fallbacks = report.fallback_report()

        assert InferenceDepKey in fallbacks.catch_all
        assert DocumentQueryDepKey in fallbacks.served_plain
        assert DocumentQueryDepKey not in fallbacks.catch_all


class TestEveryCollisionClassComposes:
    """One per class of the drift census — plain-vs-routed, plain-vs-plain, routed-vs-routed.

    The plain-vs-routed class is covered by the inference cases above; these two are the
    ones a "it follows from the first" argument would have left untested.
    """

    def test_plain_vs_plain_the_real_crypto_stack_wins(self) -> None:
        real_kms = Deps.plain({KeyManagementDepKey: "real-kms"})
        store = DepsRegistry.from_modules(MockDepsModule()).with_deps(real_kms).freeze().store

        assert store.get_provider(KeyManagementDepKey) == "real-kms"
        assert store.fallback_report().shadowed_names() == (f"{KeyManagementDepKey.name} (plain)",)

    def test_routed_vs_routed_a_real_identity_module_wins_its_route(self) -> None:
        # The mock registers identity stubs routed on "main" — the same route the real
        # identity modules default to, so this class collides head-on.
        real_authn = Deps.routed({AuthnDepKey: {"main": "real-authn"}})
        store = DepsRegistry.from_modules(MockDepsModule()).with_deps(real_authn).freeze().store

        assert store.get_provider(AuthnDepKey, route="main") == "real-authn"
        assert store.fallback_report().shadowed_names() == (f"{AuthnDepKey.name} (route 'main')",)
        # Every other identity route the mock declared is untouched.
        assert store.exists(TenantResolverDepKey, route="main")

    def test_the_standalone_mock_is_not_reported_as_hybrid(self) -> None:
        report = context_from_modules(MockDepsModule()).deps.store.fallback_report()

        assert not report.hybrid
        assert report.shadowed == ()

    def test_two_mock_modules_in_one_context_still_fail_loud(self) -> None:
        # Two fallback environments is a wiring bug, not something to resolve.
        with pytest.raises(CoreException, match="Conflicting plain dependencies"):
            context_from_modules(MockDepsModule(), MockDepsModule())


class TestCheckWiringOnAHybridContext:
    def test_wiring_passes_and_the_report_carries_the_fallbacks(self) -> None:
        def _factory(ctx: ExecutionContext) -> Handler[None, str]:
            ctx.inference.model(_LOCAL)
            ctx.document.query(_THINGS)

            return _Built()

        registry = OperationRegistry(handlers={"predict": _factory}).freeze()
        report = check_wiring(
            registry,
            lambda: context_from_modules(MockDepsModule(state=MockState()), _local_inference()),
        )

        assert report.ok
        assert report.fallbacks is not None
        # Reporting, never failing: resolving through a fallback is a legitimate hybrid.
        assert report.fallbacks.hybrid
        assert "document_query" in report.fallbacks.served_names()


class _Built(Handler[None, str]):
    async def __call__(self, _args: None) -> str:
        return "built"
