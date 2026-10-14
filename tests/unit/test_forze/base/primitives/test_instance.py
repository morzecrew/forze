"""The process's canonical instance id."""

from __future__ import annotations

import multiprocessing
import os
import socket

from forze.base.primitives import SeededEntropySource, bind_entropy_source, instance_id
from forze.base.primitives import instance as instance_module

# ----------------------- #


def _send_id(conn: object) -> None:
    conn.send(instance_id())  # type: ignore[attr-defined]


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

    def test_a_reused_pid_gets_a_new_nonce(self) -> None:
        first = instance_id()
        instance_module._IDS.clear()  # what a restarted process starts with

        try:
            assert instance_id() != first

        finally:
            instance_module._IDS.clear()
            instance_module._IDS[os.getpid()] = first

    def test_a_forked_child_gets_its_own(self) -> None:
        parent = instance_id()
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        child = context.Process(target=_send_id, args=(send,))
        child.start()

        try:
            assert receive.poll(30)
            child_id = receive.recv()

        finally:
            child.join(30)

        assert child_id != parent
        assert child_id.split(":")[1] == str(child.pid)
        assert instance_id() == parent
