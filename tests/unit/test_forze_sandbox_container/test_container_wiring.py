"""What a container route may be wired as, and what it declares once it is.

# covers: forze_sandbox_container.kernel.config (ceilings, the identity refusal, the
#         derived capability surface, ulimit narrowing)
# covers: forze_sandbox_container.execution.deps.module (the freeze-time gates)
# covers: forze_sandbox_container.kernel.client (endpoint parsing)

None of this needs a daemon: every refusal here happens before anything is created, which
is the point of putting them at freeze. What the containment is actually worth is the
integration suite's job, because reading a ``CapDrop`` list proves nothing about it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest

from forze.application.contracts.sandbox import Provenance, SandboxSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from forze_sandbox_container import (
    ConfigurableContainerSandbox,
    ContainerSandboxConfig,
    ContainerSandboxDepsModule,
    container_capabilities,
)
from forze_sandbox_container.kernel.client import endpoint

# ----------------------- #

pytestmark = pytest.mark.unit

_BLOBS = StorageSpec(name="sandbox_files")


def _config(**overrides: Any) -> ContainerSandboxConfig:
    settings: dict[str, Any] = {
        "provenance": "untrusted",
        "image": "python:3.12-slim",
        "wall_clock_ceiling": timedelta(seconds=10),
        "max_output_bytes": 64 * 1024,
        "storage": _BLOBS,
    }
    settings.update(overrides)

    return ContainerSandboxConfig(**settings)


# ....................... #


class TestTheIdentityAChildRunsAs:
    """The one config refusal that is containment rather than hygiene."""

    @pytest.mark.parametrize("identity", ["0", "root", "0:0", "root:root", " root "])
    def test_a_root_identity_is_refused_however_it_is_spelled(self, identity: str) -> None:
        with pytest.raises(CoreException) as raised:
            _config(run_as=identity)

        assert raised.value.code == "sandbox_container_root_user"
        assert raised.value.kind == ExceptionKind.CONFIGURATION

    def test_the_default_identity_is_not_root(self) -> None:
        assert _config().run_as not in ("0", "root", "0:0", "root:root")

    def test_an_unprivileged_identity_is_accepted(self) -> None:
        assert _config(run_as="1000:1000").run_as == "1000:1000"


class TestCeilingsAndPaths:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("wall_clock_ceiling", timedelta()),
            ("wall_clock_ceiling", timedelta(seconds=-1)),
            ("max_output_bytes", 0),
            ("pids_ceiling", 0),
            ("max_artifact_bytes", 0),
            ("max_artifact_count", 0),
            ("max_workspace_bytes", 0),
            ("memory_ceiling", 0),
            ("open_files_ceiling", -1),
            ("cpu_ceiling", timedelta()),
            ("connect_timeout", timedelta()),
            ("kill_grace", timedelta(seconds=-1)),
        ],
    )
    def test_a_ceiling_that_bounds_nothing_is_refused(self, field: str, value: Any) -> None:
        with pytest.raises(CoreException) as raised:
            _config(**{field: value})

        assert raised.value.code == "sandbox_ceiling_not_positive"

    def test_an_image_nobody_named_is_refused(self) -> None:
        with pytest.raises(CoreException) as raised:
            _config(image="   ")

        assert raised.value.code == "sandbox_container_image_missing"

    def test_a_relative_workspace_is_refused(self) -> None:
        with pytest.raises(CoreException) as raised:
            _config(workspace="work")

        assert raised.value.code == "sandbox_container_workspace_invalid"


class TestWhatTheRouteDeclares:
    """The capability surface is derived from the route, and every claim owes a test.

    The claims themselves are proven against a daemon; what is checked here is that the
    route's own wiring is what decides them, because a surface fixed per module would let a
    route that imposes no ceiling accept a request asking for one.
    """

    def test_a_bare_route_is_a_container_that_imposes_nothing(self) -> None:
        capabilities = container_capabilities(_config())

        assert capabilities.isolation == "container"
        assert capabilities.network == "none"
        assert not capabilities.enforces_memory
        assert not capabilities.enforces_cpu
        assert not capabilities.enforces_open_files

    def test_every_ceiling_the_route_sets_becomes_a_claim(self) -> None:
        capabilities = container_capabilities(
            _config(
                memory_ceiling=1024,
                cpu_ceiling=timedelta(seconds=5),
                open_files_ceiling=64,
            )
        )

        assert capabilities.enforces_memory
        assert capabilities.enforces_cpu
        assert capabilities.enforces_open_files

    def test_the_tier_reports_kills_and_reaps_descendants_whatever_the_route_asks(self) -> None:
        # Both are properties of the namespace and the daemon rather than of the wiring, and
        # both are what separates this tier from the process one.
        for config in (_config(), _config(memory_ceiling=1024)):
            capabilities = container_capabilities(config)

            assert capabilities.reports_resource_kill
            assert capabilities.reaps_descendants
            assert capabilities.hard_kill
            assert capabilities.supports_stream

    def test_opening_the_network_is_visible_in_the_surface(self) -> None:
        capabilities = container_capabilities(
            _config(network="egress", acknowledge_network_egress=True)
        )

        assert capabilities.network == "egress"


class TestTheCpuCeilingHasHeadroom:
    """``SIGXCPU`` at the soft limit is the only thing that makes an over-run nameable."""

    def test_the_soft_limit_lands_below_the_hard_one(self) -> None:
        limits = _config(cpu_ceiling=timedelta(seconds=4)).ulimits
        cpu = next(limit for limit in limits if limit["Name"] == "cpu")

        assert cpu["Soft"] == 4
        assert cpu["Hard"] == 5

    def test_a_sub_second_ceiling_still_bounds_something(self) -> None:
        cpu = next(
            limit
            for limit in _config(cpu_ceiling=timedelta(milliseconds=200)).ulimits
            if limit["Name"] == "cpu"
        )

        assert cpu["Soft"] == 1

    def test_a_route_with_no_cpu_ceiling_sends_no_cpu_ulimit(self) -> None:
        assert all(limit["Name"] != "cpu" for limit in _config().ulimits)


class TestTheFreezeTimeGates:
    def test_an_untrusted_route_boots_here_where_no_lower_tier_would(self) -> None:
        module = ContainerSandboxDepsModule(routes={"jobs": _config(provenance="untrusted")})

        assert module() is not None

    def test_an_unknown_provenance_is_refused_rather_than_read_as_trusted(self) -> None:
        module = ContainerSandboxDepsModule(routes={"jobs": _config(provenance="untrused")})

        with pytest.raises(CoreException) as raised:
            module()

        assert raised.value.code == "sandbox_provenance_unknown"

    def test_opening_the_network_without_saying_so_fails_the_boot(self) -> None:
        module = ContainerSandboxDepsModule(routes={"jobs": _config(network="egress")})

        with pytest.raises(CoreException) as raised:
            module()

        assert raised.value.code == "sandbox_network_egress_unacknowledged"

    def test_a_closed_network_owes_no_acknowledgment(self) -> None:
        # The difference from the subprocess adapter, which owes one unconditionally: this
        # is the first tier that can actually close the network rather than declare what it
        # cannot prevent.
        module = ContainerSandboxDepsModule(
            routes={"jobs": _config(acknowledge_network_egress=False)}
        )

        assert module() is not None

    def test_the_route_names_are_the_specs_handlers_resolve(self) -> None:
        deps = ContainerSandboxDepsModule(routes={"jobs": _config(), "reports": _config()})()

        assert deps is not None


class TestResolvingASpec:
    def test_untrusted_code_resolves_on_this_tier(self) -> None:
        ctx = context_from_modules(MockDepsModule(state=MockState()))
        port = ConfigurableContainerSandbox(config=_config())(
            ctx, SandboxSpec(name="jobs", provenance="untrusted")
        )

        assert port.sandbox_capabilities.isolation == "container"

    def test_a_spec_of_unknown_provenance_is_refused_at_resolve(self) -> None:
        ctx = context_from_modules(MockDepsModule(state=MockState()))

        with pytest.raises(CoreException) as raised:
            ConfigurableContainerSandbox(config=_config())(
                ctx, SandboxSpec(name="jobs", provenance=cast("Provenance", "untrused"))
            )

        assert raised.value.code == "sandbox_provenance_unknown"


class TestTheDaemonEndpoint:
    @pytest.mark.parametrize(
        ("docker_host", "base"),
        [
            ("unix:///var/run/docker.sock", "http://daemon"),
            ("tcp://127.0.0.1:2375", "http://127.0.0.1:2375"),
            ("tcp://127.0.0.1:2375/", "http://127.0.0.1:2375"),
            ("http://dockerd:2375", "http://dockerd:2375"),
            ("https://dockerd:2376/", "https://dockerd:2376"),
        ],
    )
    def test_every_endpoint_shape_a_route_may_name(self, docker_host: str, base: str) -> None:
        _, resolved = endpoint(docker_host)

        assert resolved == base

    @pytest.mark.parametrize("docker_host", ["/var/run/docker.sock", "ssh://host", ""])
    def test_an_endpoint_this_client_cannot_speak_is_refused(self, docker_host: str) -> None:
        with pytest.raises(CoreException) as raised:
            endpoint(docker_host)

        assert raised.value.code == "sandbox_container_endpoint_invalid"
