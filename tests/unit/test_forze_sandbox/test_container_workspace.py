"""What crosses the container boundary, in both directions, with no daemon in the way.

# covers: forze_sandbox.container.adapters.sandbox (workspace staging, declared-artifact
#         filtering, ceiling narrowing, reading how a run ended, bounded capture)

The tar is the boundary. Everything a request stages goes in through it and everything the
request declared comes back out through it, so the guards that matter — nothing lands
outside the workspace, nothing undeclared leaves, nothing unbounded is read — are guards on
tar members rather than on paths, and none of them needs a container to exercise.
"""

from __future__ import annotations

import io
import tarfile
from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.sandbox import (
    ProgramPayload,
    ResourceRequest,
    SandboxRequest,
    SandboxSpec,
)
from forze.application.contracts.storage import StorageSpec
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from forze_sandbox.container import ContainerSandbox, ContainerSandboxConfig
from forze_sandbox.container.adapters.sandbox import (
    _build_archive,  # pyright: ignore[reportPrivateUsage]
    _Capture,  # pyright: ignore[reportPrivateUsage]
    _declared_from_archive,  # pyright: ignore[reportPrivateUsage]
    _workspace_relative,  # pyright: ignore[reportPrivateUsage]
)

# ----------------------- #

pytestmark = pytest.mark.unit

_BLOBS = StorageSpec(name="sandbox_files")
_SPEC = SandboxSpec(name="jobs", provenance="untrusted")


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


def _sandbox(**overrides: Any) -> ContainerSandbox:
    return ContainerSandbox(
        spec=_SPEC,
        config=_config(**overrides),
        ctx=context_from_modules(MockDepsModule(state=MockState())),
    )


def _members(archive: bytes) -> dict[str, tarfile.TarInfo]:
    with tarfile.open(fileobj=io.BytesIO(archive)) as opened:
        return {member.name: member for member in opened.getmembers()}


def _archive(entries: dict[str, bytes], *, prefix: str = "workspace") -> io.BytesIO:
    """A workspace archive shaped the way the daemon hands one back."""

    buffer = io.BytesIO()

    with tarfile.open(fileobj=buffer, mode="w") as opened:
        root = tarfile.TarInfo(prefix)
        root.type = tarfile.DIRTYPE
        opened.addfile(root)

        for name, data in entries.items():
            entry = tarfile.TarInfo(f"{prefix}/{name}")
            entry.size = len(data)
            opened.addfile(entry, io.BytesIO(data))

    buffer.seek(0)

    return buffer


# ....................... #


class TestStagingIntoTheWorkspace:
    def test_the_workspace_itself_is_staged_owned_by_the_run(self) -> None:
        # The daemon creates a missing directory root-owned, and the child is not root, so
        # without this member every run that writes anything fails on permissions.
        members = _members(_build_archive("/workspace", None, {}, (65534, 65533)))

        assert members["workspace"].isdir()
        assert (members["workspace"].uid, members["workspace"].gid) == (65534, 65533)

    def test_the_program_and_its_inputs_land_under_the_workspace(self) -> None:
        program = ProgramPayload(interpreter=("python",), source="print(1)\n")
        members = _members(
            _build_archive("/workspace", program, {"data.csv": b"a,b\n"}, (1000, 1000))
        )

        assert members["workspace/program"].size == len("print(1)\n")
        assert members["workspace/data.csv"].size == 4
        assert all(member.uid == 1000 for member in members.values())

    def test_a_nested_input_gets_the_directories_it_needs(self) -> None:
        # A tar member whose parent is absent extracts to nothing useful, and the child then
        # fails to open an input the request staged.
        members = _members(_build_archive("/workspace", None, {"in/deep/x.bin": b"z"}, (0, 0)))

        assert members["workspace/in"].isdir()
        assert members["workspace/in/deep"].isdir()
        assert members["workspace/in/deep/x.bin"].size == 1

    def test_a_deep_workspace_path_is_created_a_level_at_a_time(self) -> None:
        members = _members(_build_archive("/srv/run/box", None, {}, (1, 1)))

        assert members["srv"].isdir()
        assert members["srv/run"].isdir()
        assert members["srv/run/box"].isdir()

    def test_staged_files_are_not_executable(self) -> None:
        members = _members(_build_archive("/workspace", None, {"x": b"1"}, (1, 1)))

        assert members["workspace/x"].mode & 0o111 == 0

    @pytest.mark.parametrize(
        ("run_as", "expected"),
        [("65534", (65534, 65534)), ("1000:2000", (1000, 2000))],
    )
    def test_the_identity_a_staged_file_belongs_to(self, run_as: str, expected: Any) -> None:
        assert _config(run_as=run_as).identity == expected


class TestCollectingWhatWasDeclared:
    def test_only_declared_names_leave_the_workspace(self) -> None:
        artifacts, skipped = _declared_from_archive(
            _archive({"out.csv": b"kept", "notes.txt": b"discarded"}),
            "workspace",
            ("*.csv",),
            1024,
            16,
        )

        assert artifacts == {"out.csv": b"kept"}
        assert skipped == ()

    def test_a_glob_matches_across_directories_the_way_path_glob_does(self) -> None:
        artifacts, _ = _declared_from_archive(
            _archive({"a/b/deep.json": b"1", "top.json": b"2"}),
            "workspace",
            ("**/*.json",),
            1024,
            16,
        )

        assert set(artifacts) == {"a/b/deep.json", "top.json"}

    def test_the_workspace_directory_itself_is_never_an_artifact(self) -> None:
        artifacts, _ = _declared_from_archive(_archive({}), "workspace", ("*", "**/*"), 1024, 16)

        assert artifacts == {}

    def test_a_member_outside_the_workspace_is_skipped(self) -> None:
        # The daemon roots the archive at the workspace, so a member elsewhere is either a
        # different path or a name trying to be one — and either way it is not an artifact
        # this request declared.
        buffer = io.BytesIO()

        with tarfile.open(fileobj=buffer, mode="w") as opened:
            for name in ("etc/passwd", "workspace/../escape.txt", "/absolute.txt"):
                entry = tarfile.TarInfo(name)
                entry.size = 1
                opened.addfile(entry, io.BytesIO(b"x"))

        buffer.seek(0)
        artifacts, _ = _declared_from_archive(buffer, "workspace", ("*", "**/*"), 1024, 16)

        assert artifacts == {}

    def test_a_link_is_never_collected_whatever_it_points_at(self) -> None:
        # A symlink collected by name would put a file from inside the image under a name
        # the request chose — the output channel becoming a read channel.
        buffer = io.BytesIO()

        with tarfile.open(fileobj=buffer, mode="w") as opened:
            link = tarfile.TarInfo("workspace/passwd.txt")
            link.type = tarfile.SYMTYPE
            link.linkname = "/etc/passwd"
            opened.addfile(link)

        buffer.seek(0)
        artifacts, _ = _declared_from_archive(buffer, "workspace", ("*.txt",), 1024, 16)

        assert artifacts == {}

    def test_an_artifact_over_the_byte_budget_is_reported_rather_than_read(self) -> None:
        artifacts, skipped = _declared_from_archive(
            _archive({"small.txt": b"ab", "huge.txt": b"x" * 100}),
            "workspace",
            ("*.txt",),
            10,
            16,
        )

        assert set(artifacts) == {"small.txt"}
        assert skipped == ("huge.txt",)

    def test_the_budget_is_shared_across_every_artifact(self) -> None:
        artifacts, skipped = _declared_from_archive(
            _archive({"a.txt": b"x" * 8, "b.txt": b"y" * 8}),
            "workspace",
            ("*.txt",),
            10,
            16,
        )

        assert set(artifacts) == {"a.txt"}
        assert skipped == ("b.txt",)

    def test_past_the_match_limit_the_whole_collection_stops_and_says_so(self) -> None:
        # A child writing a million empty files spends the worker on the match list alone,
        # where a byte ceiling never fires because nothing has any bytes.
        artifacts, skipped = _declared_from_archive(
            _archive({f"f{index}.txt": b"" for index in range(10)}),
            "workspace",
            ("*.txt",),
            1024,
            3,
        )

        assert len(artifacts) == 3
        assert skipped == ("over 3 declared matches",)

    def test_collected_names_come_back_in_a_stable_order(self) -> None:
        artifacts, _ = _declared_from_archive(
            _archive({"c.txt": b"3", "a.txt": b"1", "b.txt": b"2"}),
            "workspace",
            ("*.txt",),
            1024,
            16,
        )

        assert list(artifacts) == ["a.txt", "b.txt", "c.txt"]

    @pytest.mark.parametrize(
        ("member", "expected"),
        [
            ("workspace/out.txt", "out.txt"),
            ("workspace/a/b.txt", "a/b.txt"),
            ("workspace", None),
            ("elsewhere/out.txt", None),
            ("/workspace/out.txt", None),
            ("workspace/../out.txt", None),
        ],
    )
    def test_which_members_have_a_workspace_relative_name(
        self, member: str, expected: str | None
    ) -> None:
        assert _workspace_relative(member, "workspace") == expected


class TestNarrowingCeilings:
    def test_a_request_narrows_the_route_and_never_widens_it(self) -> None:
        box = _sandbox(
            memory_ceiling=1024,
            cpu_ceiling=timedelta(seconds=10),
            open_files_ceiling=64,
        )
        memory, ulimits = box._ceilings(  # pyright: ignore[reportPrivateUsage]
            SandboxRequest(
                command=("true",),
                resources=ResourceRequest(memory_bytes=512, cpu_seconds=4, max_open_files=999),
            )
        )
        limits = {str(limit["Name"]): limit for limit in ulimits}

        assert memory == 512
        assert limits["cpu"]["Soft"] == 4
        assert limits["nofile"]["Soft"] == 64

    def test_narrowing_the_cpu_ceiling_keeps_the_headroom_that_names_it(self) -> None:
        box = _sandbox(cpu_ceiling=timedelta(seconds=10))
        _, ulimits = box._ceilings(  # pyright: ignore[reportPrivateUsage]
            SandboxRequest(command=("true",), resources=ResourceRequest(cpu_seconds=3))
        )
        cpu = next(limit for limit in ulimits if limit["Name"] == "cpu")

        assert (cpu["Soft"], cpu["Hard"]) == (3, 4)

    def test_a_request_asking_for_nothing_gets_the_route_s_own_ceilings(self) -> None:
        box = _sandbox(memory_ceiling=2048)
        memory, _ = box._ceilings(  # pyright: ignore[reportPrivateUsage]
            SandboxRequest(command=("true",))
        )

        assert memory == 2048


class TestReadingHowARunEnded:
    def test_the_deadline_outranks_whatever_the_container_reported(self) -> None:
        box = _sandbox(memory_ceiling=1024)
        outcome, detail = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            137, {"OOMKilled": True}, "timeout", SandboxRequest(command=("true",)), 5.0, ""
        )

        assert outcome == "killed_timeout"
        assert detail is not None and "5.0s" in detail

    def test_the_daemon_s_own_oom_flag_names_the_ceiling(self) -> None:
        box = _sandbox(memory_ceiling=1024)
        outcome, _ = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            137, {"OOMKilled": True}, None, SandboxRequest(command=("true",)), 5.0, ""
        )

        assert outcome == "killed_oom"

    @pytest.mark.parametrize("status", [152, 137])
    def test_a_cpu_over_run_is_named_at_either_edge_of_its_ceiling(self, status: int) -> None:
        # `SIGXCPU` is the soft limit's warning and a child may catch it and carry on; the
        # kernel then sends `SIGKILL` at the hard limit one second later.
        box = _sandbox(cpu_ceiling=timedelta(seconds=2))
        outcome, detail = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            status, {}, None, SandboxRequest(command=("true",)), 5.0, ""
        )

        assert outcome == "killed_resource"
        assert detail is not None and "2s cpu ceiling" in detail

    def test_the_same_status_says_nothing_on_a_route_with_no_cpu_ceiling(self) -> None:
        box = _sandbox()
        outcome, detail = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            152, {}, None, SandboxRequest(command=("true",)), 5.0, ""
        )

        assert outcome == "exited"
        assert detail is None

    def test_a_non_zero_exit_names_what_was_bounding_it(self) -> None:
        box = _sandbox(memory_ceiling=4096, open_files_ceiling=32)
        outcome, detail = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            1, {}, None, SandboxRequest(command=("true",)), 5.0, ""
        )

        assert outcome == "exited"
        assert detail == "limits in force: memory=4096, nofile=32"

    def test_an_init_that_could_not_exec_is_a_spawn_failure(self) -> None:
        # The init process always starts, so 127 alone says nothing: without the marker a
        # program the image does not have would answer `exited` here and `spawn_failed` on
        # the tier below, from one plane.
        box = _sandbox()
        outcome, detail = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            127,
            {},
            None,
            SandboxRequest(command=("true",)),
            5.0,
            "[FATAL tini (7)] exec /no/such failed: No such file or directory\n",
        )

        assert outcome == "spawn_failed"
        assert detail == "[FATAL tini (7)] exec /no/such failed: No such file or directory"

    def test_a_program_that_exits_127_on_its_own_is_not_a_spawn_failure(self) -> None:
        box = _sandbox()
        outcome, _ = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            127, {}, None, SandboxRequest(command=("true",)), 5.0, "command not found\n"
        )

        assert outcome == "exited"

    def test_a_kill_outranks_the_exec_marker(self) -> None:
        box = _sandbox()
        outcome, _ = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            127, {}, "timeout", SandboxRequest(command=("true",)), 5.0, "[FATAL tini (7)] x"
        )

        assert outcome == "killed_timeout"

    def test_a_clean_exit_needs_no_explanation(self) -> None:
        box = _sandbox(memory_ceiling=4096)
        outcome, detail = box._ended_by(  # pyright: ignore[reportPrivateUsage]
            0, {}, None, SandboxRequest(command=("true",)), 5.0, ""
        )

        assert outcome == "exited"
        assert detail is None


class TestBoundedCapture:
    def test_what_is_kept_is_capped_and_what_is_handed_on_is_not(self) -> None:
        capture = _Capture(4)

        assert capture.add(b"abcdefgh") == "abcdefgh"

        stream = capture.finish()

        assert stream.text == "abcd"
        assert stream.byte_count == 8
        assert stream.truncated

    def test_a_character_split_across_two_reads_is_one_character(self) -> None:
        # Decoding each chunk on its own turns a split multi-byte character into two
        # replacement marks, which is how a log full of � starts.
        capture = _Capture(64)
        first = capture.add("é".encode()[:1])
        second = capture.add("é".encode()[1:])

        assert first + second == "é"
        assert capture.finish().text == "é"

    def test_output_inside_the_cap_is_not_flagged(self) -> None:
        capture = _Capture(16)
        capture.add(b"short")

        assert not capture.finish().truncated
