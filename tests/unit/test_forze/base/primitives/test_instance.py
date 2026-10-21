"""The process's canonical instance id."""

from __future__ import annotations

import itertools
import multiprocessing
import os
import socket
import subprocess  # nosec B404 — fixed argv, runs this interpreter
import sys

import pytest

from forze.base.primitives import (
    SeededEntropySource,
    bind_entropy_source,
    current_entropy_source,
    instance_id,
)
from forze.base.primitives import instance as instance_module

# ----------------------- #


def _send_id(conn: object) -> None:
    inherited = sorted(instance_module._IDS)
    conn.send((inherited, instance_id()))  # type: ignore[attr-defined]


class TestInstanceId:
    def test_is_host_pid_nonce(self) -> None:
        host, pid, nonce = instance_id().rsplit(":", 2)

        assert host == socket.gethostname()
        assert pid == str(os.getpid())
        assert len(nonce) == 8 and int(nonce, 16) >= 0

    def test_is_stable_for_the_process(self) -> None:
        first = instance_id()

        # A later read under another entropy source still sees the minted id.
        with bind_entropy_source(SeededEntropySource(seed=1)):
            assert instance_id() == first

    def test_minting_draws_nothing_from_a_bound_seeded_source(self) -> None:
        first = instance_id()
        instance_module._IDS.clear()  # the next call mints

        try:
            with bind_entropy_source(SeededEntropySource(seed=7)):
                instance_id()
                after_minting = [current_entropy_source().randbits(32) for _ in range(3)]

            with bind_entropy_source(SeededEntropySource(seed=7)):
                untouched = [current_entropy_source().randbits(32) for _ in range(3)]

        finally:
            instance_module._IDS.clear()
            instance_module._IDS[os.getpid()] = first

        assert after_minting == untouched

    def test_a_reused_pid_gets_a_new_nonce(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Two draws from a counter, not two random ones that could collide.
        draws = itertools.count(1)

        class _Counting:
            def randbits(self, k: int) -> int:
                return next(draws)

        first = instance_id()
        monkeypatch.setattr(instance_module, "SystemEntropySource", _Counting)

        try:
            instance_module._IDS.clear()  # what a restarted process starts with
            minted = instance_id()
            instance_module._IDS.clear()  # and again, reusing the same pid
            again = instance_id()

        finally:
            instance_module._IDS.clear()
            instance_module._IDS[os.getpid()] = first

        assert (minted.rsplit(":", 1)[1], again.rsplit(":", 1)[1]) == ("00000001", "00000002")

    def test_a_forked_child_gets_its_own(self) -> None:
        parent = instance_id()
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        child = context.Process(target=_send_id, args=(send,))
        child.start()

        try:
            assert receive.poll(30)
            inherited, child_id = receive.recv()

        finally:
            child.join(30)

        # It forgot the parent's id, so a reused pid down the line still mints a new one.
        assert inherited == []
        assert child_id != parent
        assert child_id.split(":")[1] == str(child.pid)
        assert instance_id() == parent


class TestWithoutFork:
    def test_imports_where_the_platform_has_no_fork_hook(self) -> None:
        # Windows has neither ``os.fork`` nor ``os.register_at_fork``; importing must not need them.
        script = (
            "import os; del os.fork, os.register_at_fork\n"
            "from forze.base.primitives import instance_id\n"
            "print(instance_id())\n"
        )
        result = subprocess.run(  # nosec B603 — this interpreter, a fixed script
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
        )

        assert result.returncode == 0, result.stderr
        assert len(result.stdout.strip().rsplit(":", 2)) == 3
