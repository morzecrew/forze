"""Shared helpers for secrets-lifecycle kit tests."""

from __future__ import annotations

import asyncio
from collections.abc import Collection

from forze.application.contracts.secrets import SecretChanged, SecretRef, SecretsChangeSource

# ----------------------- #


def collect_changes(
    source: SecretsChangeSource,
    out: list[SecretChanged],
    refs: Collection[SecretRef] | None = None,
) -> asyncio.Task[None]:
    """Drain a subscription into *out* on a background task (cancel it when done)."""

    async def _drain() -> None:
        async for change in source.subscribe(refs):
            out.append(change)

    return asyncio.create_task(_drain())


async def settle(rounds: int = 5) -> None:
    """Give cooperative tasks a few scheduling rounds."""

    for _ in range(rounds):
        await asyncio.sleep(0)
