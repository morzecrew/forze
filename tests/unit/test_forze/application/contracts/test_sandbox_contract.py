"""The sandbox contract: what a request may say, and what the capabilities admit to.

# covers: forze.application.contracts.sandbox.value_objects (argv-or-program, workspace
#         names that cannot climb out, positive ceilings, the bounded capture)
# covers: forze.application.contracts.sandbox.capabilities (tier ordering, the provenance
#         gate, the stream refusal)

The gate these hold up is the plane's whole claim, so the interesting cases are the ones
that *look* fine: a tier comparison done on strings, a workspace name with a ``..`` in the
middle, a request that carries both a command and a program.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.sandbox import (
    DEFAULT_SANDBOX_CAPABILITIES,
    FULL_SANDBOX_CAPABILITIES,
    MINIMUM_UNTRUSTED_ISOLATION,
    UNKNOWN_PROVENANCE_CODE,
    CapturedStream,
    ProgramPayload,
    ResourceRequest,
    SandboxCapabilities,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
    contains_untrusted,
    isolation_rank,
    validate_provenance,
    validate_stream_supported,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit

# ----------------------- #


def _program() -> ProgramPayload:
    return ProgramPayload(interpreter=("python3",), source="print(1)")


# ----------------------- #


class TestWhatARequestMaySay:
    def test_a_request_runs_a_command_or_a_program_and_not_both(self) -> None:
        with pytest.raises(CoreException) as caught:
            SandboxRequest(command=("echo", "hi"), program=_program())

        assert caught.value.code == "sandbox_request_command_ambiguous"

    def test_a_request_that_names_nothing_to_run_is_refused(self) -> None:
        with pytest.raises(CoreException):
            SandboxRequest()

    def test_a_program_becomes_argv(self) -> None:
        # The no-shell rule made concrete: the source is a file and the interpreter is
        # executed on it, so nothing is ever handed to a shell to re-parse.
        request = SandboxRequest(
            program=ProgramPayload(
                interpreter=("python3", "-u"), source="print(1)", filename="job.py", args=("--x",)
            )
        )

        assert request.argv == ("python3", "-u", "job.py", "--x")

    def test_an_interpreter_with_nothing_in_it_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            ProgramPayload(interpreter=(), source="print(1)")

        assert caught.value.code == "sandbox_program_interpreter_empty"

    @pytest.mark.parametrize("filename", ["../evil.py", "/tmp/evil.py", "~/evil.py"])
    def test_a_program_filename_that_leaves_the_workspace_is_refused(self, filename: str) -> None:
        # The program is written before the child starts, so an escaping filename is a
        # write to wherever the worker can reach — under the caller's own control.
        with pytest.raises(CoreException) as caught:
            ProgramPayload(interpreter=("python3",), source="print(1)", filename=filename)

        assert caught.value.code == "sandbox_workspace_name_escapes"

    @pytest.mark.parametrize(
        "name",
        ["/etc/passwd", "../escape", "nested/../../escape", "~/.ssh/id_rsa", "", "  padded"],
    )
    def test_a_staged_name_that_could_leave_the_workspace_is_refused(self, name: str) -> None:
        # The workspace boundary is not a boundary if the names crossing it can climb out:
        # a staged input written to `../..` is a write anywhere the worker can reach.
        with pytest.raises(CoreException) as caught:
            SandboxRequest(command=("echo",), input_files={name: "key"})

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize("glob", ["/tmp/*", "../*.txt"])
    def test_an_output_glob_that_reaches_outside_is_refused(self, glob: str) -> None:
        with pytest.raises(CoreException):
            SandboxRequest(command=("echo",), output_globs=(glob,))

    def test_a_nested_workspace_name_is_fine(self) -> None:
        request = SandboxRequest(command=("echo",), input_files={"in/data/x.csv": "key"})

        assert "in/data/x.csv" in request.input_files

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"memory_bytes": 0},
            {"cpu_seconds": -1},
            {"max_output_bytes": 0},
            {"max_open_files": -2},
            {"wall_clock": timedelta()},
        ],
    )
    def test_a_non_positive_ceiling_is_refused_rather_than_guessed(self, kwargs: dict) -> None:
        # Zero reads as "no limit" on some backends and "refuse everything" on others, and
        # a request cannot mean both.
        with pytest.raises(CoreException) as caught:
            ResourceRequest(**kwargs)

        assert caught.value.code == "sandbox_resource_not_positive"

    def test_a_non_positive_timeout_is_refused(self) -> None:
        with pytest.raises(CoreException):
            SandboxRequest(command=("echo",), timeout=timedelta(seconds=-1))


class TestWhatComesBackIsJournalable:
    def test_a_result_is_json_trivial(self) -> None:
        # Durable steps journal this verbatim: text and numbers, files as keys, nothing
        # that needs a codec to survive a round trip.
        import json

        import attrs

        result = SandboxResult(
            outcome="exited",
            exit_code=0,
            stdout=CapturedStream(text="hi", byte_count=2),
            output_files={"out.txt": "storage-key"},
        )

        assert json.loads(json.dumps(attrs.asdict(result), default=str))["exit_code"] == 0

    def test_success_is_narrow_on_purpose(self) -> None:
        exited_badly = SandboxResult(outcome="exited", exit_code=1)
        killed = SandboxResult(outcome="killed_timeout")

        assert not exited_badly.succeeded
        assert not killed.succeeded
        assert SandboxResult(outcome="exited", exit_code=0).succeeded


class TestTheTierOrdering:
    def test_tiers_rank_by_containment_and_not_alphabetically(self) -> None:
        # The bug this forbids: comparing tier strings, where "container" < "none" and
        # every untrusted route passes the gate.
        ranks = [isolation_rank(tier) for tier in ("none", "process", "container", "vm")]

        assert ranks == sorted(ranks)
        assert ranks == sorted(set(ranks))

    def test_only_container_and_above_may_carry_untrusted_code(self) -> None:
        assert not contains_untrusted("none")
        assert not contains_untrusted("process")
        assert contains_untrusted("container")
        assert contains_untrusted("vm")
        assert MINIMUM_UNTRUSTED_ISOLATION == "container"


class TestTheProvenanceGate:
    @pytest.mark.parametrize("isolation", ["none", "process"])
    def test_untrusted_code_is_refused_by_an_adapter_that_cannot_contain_it(
        self, isolation: str
    ) -> None:
        with pytest.raises(CoreException) as caught:
            validate_provenance(
                provenance="untrusted",
                capabilities=SandboxCapabilities(isolation=isolation),  # type: ignore[arg-type]
                backend="probe",
                route="jobs",
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "sandbox_untrusted_underisolated"
        assert "process-tier limits bound" in str(caught.value)

    @pytest.mark.parametrize("isolation", ["container", "vm"])
    def test_untrusted_code_passes_where_it_is_contained(self, isolation: str) -> None:
        validate_provenance(
            provenance="untrusted",
            capabilities=SandboxCapabilities(isolation=isolation),  # type: ignore[arg-type]
            backend="probe",
            route="jobs",
        )

    def test_trusted_code_runs_anywhere(self) -> None:
        # The gate is about the threat declaration, not about making every route a
        # container: an app running its own binaries is the plane's weakest tier by design.
        validate_provenance(
            provenance="trusted",
            capabilities=DEFAULT_SANDBOX_CAPABILITIES,
            backend="probe",
            route="jobs",
        )

    @pytest.mark.parametrize(
        "provenance",
        ["untrused", "TRUSTED", "", "none", "unknown"],
    )
    def test_a_provenance_nobody_declared_is_refused_on_every_tier(self, provenance: str) -> None:
        # The gate used to read "not untrusted" as trusted, so a typo, an empty string or a
        # value rebuilt from JSON walked straight past it. `Provenance` is a `Literal`,
        # which mypy checks and the interpreter does not, so the vocabulary has to be
        # enforced here or nowhere. Checked against the *strongest* surface, because the
        # refusal is about the value rather than about the containment.
        with pytest.raises(CoreException) as caught:
            validate_provenance(
                provenance=provenance,
                capabilities=FULL_SANDBOX_CAPABILITIES,
                backend="probe",
                route="jobs",
            )

        assert caught.value.code == UNKNOWN_PROVENANCE_CODE

    def test_provenance_has_no_default_on_the_spec(self) -> None:
        # A plane whose value is a threat declaration cannot let it be forgotten into the
        # safe-looking default.
        with pytest.raises(TypeError):
            SandboxSpec(name="jobs")  # type: ignore[call-arg]

        assert SandboxSpec(name="jobs", provenance="untrusted").provenance == "untrusted"


class TestCapabilityHonesty:
    def test_the_default_surface_claims_almost_nothing(self) -> None:
        assert DEFAULT_SANDBOX_CAPABILITIES.isolation == "none"
        assert not DEFAULT_SANDBOX_CAPABILITIES.enforces_memory
        assert not DEFAULT_SANDBOX_CAPABILITIES.reaps_descendants
        assert not DEFAULT_SANDBOX_CAPABILITIES.supports_stream

    def test_the_full_surface_is_the_mocks_to_borrow(self) -> None:
        assert FULL_SANDBOX_CAPABILITIES.isolation == "vm"
        assert FULL_SANDBOX_CAPABILITIES.supports_stream

    def test_streaming_is_refused_where_it_is_not_served(self) -> None:
        with pytest.raises(CoreException) as caught:
            validate_stream_supported(DEFAULT_SANDBOX_CAPABILITIES, backend="probe")

        assert caught.value.code == "sandbox_feature_unsupported"

        validate_stream_supported(FULL_SANDBOX_CAPABILITIES, backend="probe")
