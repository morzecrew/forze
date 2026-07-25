"""Directory change source: stat-gated hashing, symlink swaps, traversal guard."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from forze.application.contracts.secrets import SecretChanged, SecretRef, content_secret_version
from forze.base.exceptions import CoreException
from forze_kits.integrations.secrets import DirectorySecretsChangeSource

from ._helpers import collect_changes, settle

# ----------------------- #

_REF = SecretRef("dsn")


class TestDirectorySource:
    async def test_prime_then_rewrite_emits_content_version(self, tmp_path: Path) -> None:
        (tmp_path / "dsn").write_text("dsn-a", encoding="utf-8")
        source = DirectorySecretsChangeSource(root=tmp_path, refs=(_REF,))
        seen: list[SecretChanged] = []
        task = collect_changes(source, seen)
        await settle()

        try:
            await source.tick()
            assert seen == []

            (tmp_path / "dsn").write_text("dsn-b", encoding="utf-8")
            # In-place rewrites within the filesystem's timestamp granularity are
            # invisible to the stat gate (kubelet swaps change the inode instead);
            # model time passing explicitly.
            os.utime(tmp_path / "dsn", ns=(1_000_000_000, 2_000_000_000))
            await source.tick()
            await settle()

            assert [change.version for change in seen] == [content_secret_version("dsn-b")]

        finally:
            task.cancel()

    async def test_touch_without_content_change_stays_silent(self, tmp_path: Path) -> None:
        target = tmp_path / "dsn"
        target.write_text("dsn-a", encoding="utf-8")
        source = DirectorySecretsChangeSource(root=tmp_path, refs=(_REF,))
        seen: list[SecretChanged] = []
        task = collect_changes(source, seen)
        await settle()

        try:
            await source.tick()
            # Same bytes, new mtime: the stat gate re-reads, the hash says no change.
            target.write_text("dsn-a", encoding="utf-8")
            await source.tick()
            await settle()

            assert seen == []

        finally:
            task.cancel()

    async def test_symlink_swap_is_observed(self, tmp_path: Path) -> None:
        # Model kubelet's atomic ..data swap: the watched path is a symlink whose
        # target directory is replaced wholesale.
        (tmp_path / "..data_v1").mkdir()
        (tmp_path / "..data_v1" / "dsn").write_text("dsn-a", encoding="utf-8")
        (tmp_path / "..data_v2").mkdir()
        (tmp_path / "..data_v2" / "dsn").write_text("dsn-b", encoding="utf-8")
        (tmp_path / "..data").symlink_to(tmp_path / "..data_v1", target_is_directory=True)

        source = DirectorySecretsChangeSource(root=tmp_path, refs=(SecretRef("..data/dsn"),))
        seen: list[SecretChanged] = []
        task = collect_changes(source, seen)
        await settle()

        try:
            await source.tick()
            assert seen == []

            swap = tmp_path / "..data.tmp"
            swap.symlink_to(tmp_path / "..data_v2", target_is_directory=True)
            swap.rename(tmp_path / "..data")

            await source.tick()
            await settle()

            assert [change.version for change in seen] == [content_secret_version("dsn-b")]

        finally:
            task.cancel()

    async def test_missing_file_skipped_then_creation_emits(self, tmp_path: Path) -> None:
        source = DirectorySecretsChangeSource(root=tmp_path, refs=(_REF,))
        seen: list[SecretChanged] = []
        task = collect_changes(source, seen)
        await settle()

        try:
            await source.tick()  # nothing on disk yet — primes empty

            (tmp_path / "dsn").write_text("dsn-a", encoding="utf-8")
            await source.tick()
            await settle()

            assert [change.ref for change in seen] == [_REF]

        finally:
            task.cancel()

    def test_traversal_is_rejected_at_construction(self, tmp_path: Path) -> None:
        with pytest.raises(CoreException, match="escapes"):
            DirectorySecretsChangeSource(root=tmp_path, refs=(SecretRef("../outside"),))

    def test_needs_at_least_one_ref(self, tmp_path: Path) -> None:
        with pytest.raises(CoreException, match="at least one ref"):
            DirectorySecretsChangeSource(root=tmp_path, refs=())


class TestNativeEvents:
    async def test_native_events_accelerate_the_tick(self, tmp_path: Path) -> None:
        """A rewrite is observed via OS events alone — no periodic tick wired."""

        import asyncio
        from datetime import timedelta

        from forze_mock import MockDepsModule
        from tests.support.execution_context import context_from_modules

        pytest.importorskip("watchfiles")

        target = tmp_path / "dsn"
        target.write_text("dsn-a", encoding="utf-8")
        source = DirectorySecretsChangeSource(root=tmp_path, refs=(_REF,))
        seen: list[SecretChanged] = []
        task = collect_changes(source, seen)
        await settle()

        await source.tick()  # prime

        ctx = context_from_modules(MockDepsModule())
        step = source.native_events_lifecycle_step(debounce=timedelta(milliseconds=50))
        await step.startup(ctx)

        try:
            # Give the watcher a moment to arm before the mutation.
            await asyncio.sleep(0.2)
            target.write_text("dsn-b", encoding="utf-8")
            os.utime(target, ns=(1_000_000_000, 2_000_000_000))

            for _ in range(100):
                await asyncio.sleep(0.05)

                if seen:
                    break

            assert [change.version for change in seen] == [content_secret_version("dsn-b")]

        finally:
            await step.shutdown(ctx)
            task.cancel()

        assert step.requires_long_running

    def test_fails_closed_without_watchfiles(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_kits.integrations.secrets import file_source

        monkeypatch.setattr(file_source, "_awatch", None)
        source = DirectorySecretsChangeSource(root=tmp_path, refs=(_REF,))

        with pytest.raises(CoreException, match="watchfiles") as excinfo:
            source.native_events_lifecycle_step()

        assert "forze[watchfiles]" in excinfo.value.summary

    def test_rejects_non_positive_debounce(self, tmp_path: Path) -> None:
        from datetime import timedelta

        pytest.importorskip("watchfiles")
        source = DirectorySecretsChangeSource(root=tmp_path, refs=(_REF,))

        with pytest.raises(CoreException, match="Debounce"):
            source.native_events_lifecycle_step(debounce=timedelta(0))
