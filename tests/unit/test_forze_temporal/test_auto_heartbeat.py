"""Unit tests for the auto-heartbeat interval and its wiring.

The pump itself only means anything against a running worker (see the integration leg);
what is checked here is the arithmetic and the two cases that must produce *no* pump.
"""

from datetime import timedelta

import pytest

pytest.importorskip("temporalio")

from forze.application.execution import Deps
from forze.testing import context_from_deps
from forze_temporal.interceptors.context import (
    ActivityContextInboundInterceptor,
    ExecutionContextInterceptor,
    heartbeat_interval,
)

# ----------------------- #


def _interceptor(**kwargs) -> ExecutionContextInterceptor:
    ctx = context_from_deps(Deps.plain({}))

    return ExecutionContextInterceptor(ctx_dep=lambda: ctx, **kwargs)


# ----------------------- #


class TestHeartbeatInterval:
    """When to beat, and how often."""

    def test_a_third_of_the_timeout(self) -> None:
        """Two beats can be lost before the server calls the activity dead."""

        assert heartbeat_interval(timedelta(seconds=9), is_local=False) == 3.0

    def test_no_timeout_means_no_pump(self) -> None:
        """Nothing to miss: an activity without a heartbeat timeout has no deadline.

        Beating anyway would be pure RPC cost for a guarantee nobody asked for.
        """

        assert heartbeat_interval(None, is_local=False) is None

    def test_local_activities_never_beat(self) -> None:
        """A local activity runs inside the workflow task and cannot heartbeat.

        ``activity.heartbeat()`` is a no-op there, so a pump would burn a task per local
        activity while implying a liveness guarantee that does not exist.
        """

        assert heartbeat_interval(timedelta(seconds=9), is_local=True) is None

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_a_non_positive_timeout_means_no_pump(self, timeout: timedelta) -> None:
        """Guards the divisor: a zero interval would be a `sleep(0)` hot loop."""

        assert heartbeat_interval(timeout, is_local=False) is None


# ....................... #


class TestWiring:
    """The flag is opt-in and reaches the activity interceptor."""

    def test_off_by_default(self) -> None:
        """An automatic heartbeat masks a wedged activity, so it is a decision."""

        assert _interceptor().auto_heartbeat is False

    @pytest.mark.parametrize("enabled", [True, False])
    def test_flag_reaches_the_activity_interceptor(self, enabled: bool) -> None:
        inner = _interceptor(auto_heartbeat=enabled).intercept_activity(
            ActivityContextInboundInterceptor(
                next=None,  # type: ignore[arg-type]
                ctx_dep=lambda: None,  # type: ignore[arg-type,return-value]
            ),
        )

        assert isinstance(inner, ActivityContextInboundInterceptor)
        assert inner.auto_heartbeat is enabled

    def test_activity_interceptor_defaults_to_off(self) -> None:
        """A hand-built interceptor inherits the same honest default."""

        built = ActivityContextInboundInterceptor(
            next=None,  # type: ignore[arg-type]
            ctx_dep=lambda: None,  # type: ignore[arg-type,return-value]
        )

        assert built.auto_heartbeat is False
