"""Unit tests for :class:`~forze_temporal.TemporalStartOptions` and its wiring."""

import inspect
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import attrs

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException, ExceptionKind

pytest.importorskip("temporalio")

from temporalio.client import Client
from temporalio.common import RetryPolicy, TypedSearchAttributes, WorkflowIDReusePolicy

from forze.application.contracts.durable.workflow import DurableWorkflowSpec
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze_temporal import TemporalStartOptions, TemporalWorkflowConfig
from forze_temporal.adapters.workflow import TemporalWorkflowCommandAdapter
from forze_temporal.kernel.client.client import TemporalClient

# ----------------------- #


class _Arg(BaseModel):
    n: int = 1


_SPEC = DurableWorkflowSpec[_Arg, _Arg](
    name="wf",
    run=DurableWorkflowInvokeSpec(args_type=_Arg, return_type=_Arg),
)


def _adapter(
    client: object,
    *,
    start_options: TemporalStartOptions | None = None,
) -> TemporalWorkflowCommandAdapter[_Arg, _Arg]:
    return TemporalWorkflowCommandAdapter(
        client=client,  # type: ignore[arg-type]
        queue="tq",
        spec=_SPEC,
        tenant_aware=False,
        start_options=start_options,
    )


def _recording_client() -> MagicMock:
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=MagicMock(id="wf-1", run_id="run-1"))

    return client


# ----------------------- #


class TestOverride:
    """Field-by-field merge — the point of a per-call override."""

    def test_override_with_none_returns_self(self) -> None:
        """No override means the configured set, unchanged and un-copied."""

        base = TemporalStartOptions(run_timeout=timedelta(seconds=30))

        assert base.override(None) is base

    def test_override_only_replaces_fields_it_sets(self) -> None:
        """The fields the caller did not mention survive — that is the whole feature.

        A merge that replaced the object wholesale would silently drop the workflow
        kind's other configured options at every per-call override.
        """

        base = TemporalStartOptions(
            run_timeout=timedelta(seconds=60),
            execution_timeout=timedelta(seconds=120),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        merged = base.override(TemporalStartOptions(run_timeout=timedelta(seconds=30)))

        assert merged.run_timeout == timedelta(seconds=30)
        assert merged.execution_timeout == timedelta(seconds=120)
        assert merged.id_reuse_policy is WorkflowIDReusePolicy.REJECT_DUPLICATE

    def test_override_cannot_unset_a_configured_field(self) -> None:
        """``None`` reads as *unspecified*, never as "clear what config declared"."""

        base = TemporalStartOptions(run_timeout=timedelta(seconds=60))

        assert base.override(TemporalStartOptions()).run_timeout == timedelta(seconds=60)

    def test_override_leaves_the_base_untouched(self) -> None:
        """Frozen: the configured set is shared across every call on the adapter."""

        base = TemporalStartOptions(run_timeout=timedelta(seconds=60))
        base.override(TemporalStartOptions(run_timeout=timedelta(seconds=5)))

        assert base.run_timeout == timedelta(seconds=60)

    def test_override_covers_every_field(self) -> None:
        """Each field is independently overridable — no field is left out of the merge."""

        full = TemporalStartOptions(
            retry_policy=RetryPolicy(maximum_attempts=3),
            execution_timeout=timedelta(minutes=10),
            run_timeout=timedelta(minutes=5),
            task_timeout=timedelta(seconds=20),
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            memo={"team": "billing"},
            search_attributes=TypedSearchAttributes.empty,
            start_delay=timedelta(seconds=1),
        )
        merged = TemporalStartOptions().override(full)

        assert merged == full


# ....................... #


class TestAsStartKwargs:
    """The mapping onto ``Client.start_workflow``."""

    def test_unset_fields_are_absent(self) -> None:
        """Not ``None``-valued — absent, so the SDK's own defaults still apply."""

        assert TemporalStartOptions().as_start_kwargs() == {}

    def test_set_fields_are_forwarded_verbatim(self) -> None:
        policy = RetryPolicy(maximum_attempts=2)
        options = TemporalStartOptions(
            retry_policy=policy,
            run_timeout=timedelta(seconds=30),
        )

        assert options.as_start_kwargs() == {
            "retry_policy": policy,
            "run_timeout": timedelta(seconds=30),
        }

    def test_every_key_is_a_real_start_workflow_parameter(self) -> None:
        """Pins the field-name/kwarg-name coupling this mapping quietly relies on.

        ``as_start_kwargs`` forwards attribute names straight through, so an SDK that
        renames a parameter (or a field added here under a name the SDK does not know)
        turns into a ``TypeError`` at the first start in production. Here it is a
        failing test instead.
        """

        full = TemporalStartOptions(
            retry_policy=RetryPolicy(maximum_attempts=3),
            execution_timeout=timedelta(minutes=10),
            run_timeout=timedelta(minutes=5),
            task_timeout=timedelta(seconds=20),
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            memo={"team": "billing"},
            search_attributes=TypedSearchAttributes.empty,
            start_delay=timedelta(seconds=1),
        )
        accepted = set(inspect.signature(Client.start_workflow).parameters)

        assert set(full.as_start_kwargs()) <= accepted
        # Every field participates, so the check above covers the whole value object.
        assert len(full.as_start_kwargs()) == len(attrs.fields(TemporalStartOptions))


# ....................... #


class TestValidation:
    """Non-positive timeouts are refused at construction, not at the server."""

    @pytest.mark.parametrize(
        "field",
        ["execution_timeout", "run_timeout", "task_timeout"],
    )
    @pytest.mark.parametrize("value", [timedelta(0), timedelta(seconds=-1)])
    def test_non_positive_timeout_is_a_configuration_error(
        self,
        field: str,
        value: timedelta,
    ) -> None:
        """Temporal reads a non-positive timeout as *unset* — silently unbounding it."""

        with pytest.raises(CoreException, match="must be positive") as excinfo:
            TemporalStartOptions(**{field: value})  # type: ignore[arg-type]

        assert excinfo.value.kind is ExceptionKind.CONFIGURATION

    def test_negative_start_delay_is_refused(self) -> None:
        with pytest.raises(CoreException, match="cannot be negative"):
            TemporalStartOptions(start_delay=timedelta(seconds=-1))

    def test_zero_start_delay_is_allowed(self) -> None:
        """Zero delay is a meaningful value (start now), unlike a zero timeout."""

        assert TemporalStartOptions(start_delay=timedelta(0)).start_delay == timedelta(0)


# ....................... #


class TestClientForwarding:
    """:meth:`TemporalClient.start_workflow` passes options through to the SDK."""

    @staticmethod
    def _connected(backend: MagicMock) -> TemporalClient:
        client = TemporalClient()
        object.__setattr__(client, "_TemporalClient__client", backend)

        return client

    @pytest.mark.asyncio
    async def test_no_options_sends_exactly_the_historical_kwargs(self) -> None:
        """Wiring that declares no options must produce the pre-existing request."""

        backend = MagicMock()
        backend.start_workflow = AsyncMock(return_value=MagicMock())

        await self._connected(backend).start_workflow(
            "tq",
            "wf",
            _Arg(),
            workflow_id="wf-1",
        )

        assert set(backend.start_workflow.await_args.kwargs) == {
            "workflow",
            "id",
            "task_queue",
            "arg",
        }

    @pytest.mark.asyncio
    async def test_options_reach_the_sdk_call(self) -> None:
        backend = MagicMock()
        backend.start_workflow = AsyncMock(return_value=MagicMock())

        await self._connected(backend).start_workflow(
            "tq",
            "wf",
            _Arg(),
            workflow_id="wf-1",
            options=TemporalStartOptions(
                run_timeout=timedelta(seconds=30),
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            ),
        )

        kwargs = backend.start_workflow.await_args.kwargs
        assert kwargs["run_timeout"] == timedelta(seconds=30)
        assert kwargs["id_reuse_policy"] is WorkflowIDReusePolicy.REJECT_DUPLICATE


# ....................... #


class TestAdapterMerging:
    """The adapter is where the configured default and the per-call set meet."""

    @pytest.mark.asyncio
    async def test_configured_options_reach_the_client(self) -> None:
        client = _recording_client()
        options = TemporalStartOptions(run_timeout=timedelta(seconds=60))

        await _adapter(client, start_options=options).start(_Arg())

        assert client.start_workflow.await_args.kwargs["options"] is options

    @pytest.mark.asyncio
    async def test_per_call_override_wins_field_by_field(self) -> None:
        client = _recording_client()
        adapter = _adapter(
            client,
            start_options=TemporalStartOptions(
                run_timeout=timedelta(seconds=60),
                execution_timeout=timedelta(seconds=120),
            ),
        )

        await adapter.start(
            _Arg(),
            options=TemporalStartOptions(run_timeout=timedelta(seconds=30)),
        )

        sent = client.start_workflow.await_args.kwargs["options"]
        assert sent.run_timeout == timedelta(seconds=30)
        assert sent.execution_timeout == timedelta(seconds=120)

    @pytest.mark.asyncio
    async def test_per_call_options_without_a_configured_default(self) -> None:
        """An unconfigured workflow kind still honours a per-call set."""

        client = _recording_client()
        options = TemporalStartOptions(run_timeout=timedelta(seconds=30))

        await _adapter(client).start(_Arg(), options=options)

        assert client.start_workflow.await_args.kwargs["options"] is options

    @pytest.mark.asyncio
    async def test_no_options_anywhere_sends_none(self) -> None:
        """Neither side configured: nothing is synthesized."""

        client = _recording_client()

        await _adapter(client).start(_Arg())

        assert client.start_workflow.await_args.kwargs["options"] is None


# ....................... #


class TestConfigWiring:
    """:class:`TemporalWorkflowConfig` carries the options to the adapter factory."""

    def test_start_options_default_to_none(self) -> None:
        assert TemporalWorkflowConfig(queue="tq").start_options is None

    def test_factory_threads_configured_options_onto_the_adapter(self) -> None:
        from forze.application.execution import Deps
        from forze_temporal.execution.deps.factories.workflow import (
            ConfigurableTemporalWorkflowCommand,
        )
        from forze_temporal.execution.deps.keys import TemporalClientDepKey
        from tests.support.execution_context import context_from_deps

        options = TemporalStartOptions(task_timeout=timedelta(seconds=20))
        sentinel = MagicMock()
        ctx = context_from_deps(Deps.plain({TemporalClientDepKey: sentinel}))

        factory = ConfigurableTemporalWorkflowCommand(
            config=TemporalWorkflowConfig(queue="tq", start_options=options),
        )
        adapter = factory(ctx, _SPEC)

        assert adapter.start_options is options


# ....................... #


@pytest.mark.asyncio
async def test_start_options_are_absent_from_the_engine_agnostic_port() -> None:
    """The contract port must not learn Temporal vocabulary.

    A caller holding ``DurableWorkflowCommandPort`` sees a signature without *options*;
    only a caller who knows the engine can reach it on the adapter.
    """

    from forze.application.contracts.durable.workflow import DurableWorkflowCommandPort

    assert "options" not in inspect.signature(DurableWorkflowCommandPort.start).parameters
    assert "options" in inspect.signature(TemporalWorkflowCommandAdapter.start).parameters


@pytest.mark.asyncio
async def test_connect_kwargs_are_untouched_by_the_new_option_set() -> None:
    """Start options ride the start call, never the connection."""

    with patch(
        "forze_temporal.kernel.client.client.Client.connect",
        new_callable=AsyncMock,
        return_value=MagicMock(),
    ) as connect:
        client = TemporalClient()
        await client.initialize("localhost:7233")

    assert set(connect.await_args.kwargs) == {
        "namespace",
        "lazy",
        "data_converter",
        "interceptors",
    }
