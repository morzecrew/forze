"""``forze ops call`` in a real process: the stdout descriptor, signals, and crash output.

These need what a test runner's captured streams cannot give: a file descriptor 1 to
redirect, and a process to signal. Each spawns the console entry point against the sample
application in :mod:`tests.support.ops_cli_app`.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

# ----------------------- #

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")

_ROOT = Path(__file__).resolve().parents[3]
_ENTRY = "from forze_cli import main; main()"
_STARTED = "ops-sample: slow started"


def _spawn(
    *args: str, app_name: str = "STATIC", shutdown_seconds: float = 0
) -> subprocess.Popen[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("FORZE_")}
    env["OPS_SAMPLE_SHUTDOWN_SECONDS"] = str(shutdown_seconds)

    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            _ENTRY,
            "ops",
            "call",
            *args,
            "--app",
            f"tests.support.ops_cli_app:{app_name}",
        ],
        cwd=_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_for(process: subprocess.Popen[str], marker: str = _STARTED) -> list[str]:
    assert process.stderr is not None
    seen: list[str] = []
    deadline = time.monotonic() + 30

    while time.monotonic() < deadline:
        line = process.stderr.readline()

        if not line:
            break

        seen.append(line)

        if marker in line:
            return seen

    process.kill()
    pytest.fail(f"never saw {marker!r}: {''.join(seen)}")


def _finish(process: subprocess.Popen[str], seen: list[str]) -> tuple[int, Any, str]:
    stdout, stderr = process.communicate(timeout=30)
    lines = stdout.splitlines()
    assert len(lines) == 1, stdout

    return process.returncode, json.loads(lines[0]), "".join(seen) + stderr


def _slow(seconds: float, *, after_commit: bool = False) -> str:
    return json.dumps({"seconds": seconds, "after_commit": after_commit})


# ....................... #


class TestStdout:
    def test_an_application_writing_to_stdout_leaves_one_document(self) -> None:
        process = _spawn("probe.chatty", app_name="ANONYMOUS")
        stdout, stderr = process.communicate(timeout=60)

        assert process.returncode == 0, stderr
        assert stdout == '{"text":"clean"}\n'

        for line in (
            "printed during the call",
            "written during the call",
            "logged during the call",
        ):
            assert line in stderr


class TestSignals:
    @pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
    def test_a_call_finishing_within_the_grace_keeps_its_exit_code(self, signum: int) -> None:
        process = _spawn("probe.slow", "--input", _slow(0.5), "--grace", "20")
        seen = _wait_for(process)
        process.send_signal(signum)

        code, document, stderr = _finish(process, seen)

        assert code == 0
        assert document is None
        assert "ops-sample: shutdown ran" in stderr

    def test_a_first_signal_during_shutdown_lets_it_finish(self) -> None:
        process = _spawn("probe.bound", "--grace", "0.1", shutdown_seconds=1.5)
        seen = _wait_for(process, "ops-sample: shutdown started")
        process.send_signal(signal.SIGTERM)

        code, document, stderr = _finish(process, seen)

        assert code == 0
        assert document["principal_id"] is not None
        assert "ops-sample: shutdown ran" in stderr

    def test_a_second_signal_before_the_commit_interrupts(self) -> None:
        process = _spawn("probe.slow", "--input", _slow(60), "--grace", "60")
        seen = _wait_for(process)
        process.send_signal(signal.SIGINT)
        time.sleep(0.2)
        process.send_signal(signal.SIGINT)

        code, document, stderr = _finish(process, seen)

        assert code == 130
        assert document["error"]["kind"] == "interrupted"
        assert document["error"]["detail"] == "Interrupted by SIGINT"
        assert document["error"]["retryable"] is False
        assert "ops-sample: shutdown ran" in stderr

    def test_an_expired_grace_interrupts_with_the_signal_code(self) -> None:
        process = _spawn("probe.slow", "--input", _slow(60), "--grace", "0.2")
        seen = _wait_for(process)
        process.send_signal(signal.SIGTERM)

        code, document, stderr = _finish(process, seen)

        assert code == 143
        assert document["error"]["code"] == "interrupted"
        assert "ops-sample: shutdown ran" in stderr

    def test_a_cancel_after_the_commit_point_is_ambiguous(self) -> None:
        process = _spawn("probe.slow", "--input", _slow(60, after_commit=True), "--grace", "0")
        seen = _wait_for(process)
        process.send_signal(signal.SIGTERM)

        code, document, stderr = _finish(process, seen)

        assert code == 1
        assert document["error"]["code"] == "commit_ambiguous"
        assert document["error"]["retryable"] is False
        assert "ops-sample: shutdown ran" in stderr

    @pytest.mark.parametrize(
        ("app_name", "expected_code", "error_code"),
        [
            ("ANONYMOUS", 143, "interrupted"),
            ("UNENFORCED_READ_ONLY", 1, "commit_ambiguous"),
        ],
    )
    def test_a_cancel_after_a_querys_read(
        self, app_name: str, expected_code: int, error_code: str
    ) -> None:
        process = _spawn(
            "probe.read", "--input", '{"seconds": 60}', "--grace", "0", app_name=app_name
        )
        seen = _wait_for(process)
        process.send_signal(signal.SIGTERM)

        code, document, _stderr = _finish(process, seen)

        assert code == expected_code
        assert document["error"]["code"] == error_code


class TestCrashOutput:
    def test_an_unhandled_failure_prints_no_locals(self) -> None:
        process = _spawn("probe.bound", app_name="BROKEN_RUNTIME")
        stdout, stderr = process.communicate(timeout=60)

        assert process.returncode == 1
        assert json.loads(stdout)["error"]["code"] == "core.internal"
        assert "marker-in-a-factory-local" not in stdout + stderr

    def test_a_refusing_declaration_prints_no_locals(self) -> None:
        process = _spawn("probe.echo", app_name="unbuildable")
        stdout, stderr = process.communicate(timeout=60)

        assert process.returncode == 78
        assert stdout == ""
        assert "the declaration refuses itself" in stderr
        assert "marker-in-a-local" not in stderr
