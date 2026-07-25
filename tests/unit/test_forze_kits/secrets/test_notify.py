"""Notification fan-out: outbox staging, pub/sub source translation, end-to-end relay."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from datetime import timedelta

from forze.application.contracts.pubsub import PubSubMessage, PubSubQueryDepKey
from forze.application.contracts.secrets import (
    SecretChanged,
    SecretRef,
    SecretRotated,
)
from forze.base.primitives import utcnow
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.secrets import (
    DEFAULT_SECRET_ROTATIONS_CHANNEL,
    PubSubSecretsChangeSource,
    publish_secret_rotated,
    secret_rotated_outbox_spec,
    secret_rotated_pubsub_spec,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _rotated(path: str, token: str = "v7") -> SecretRotated:
    return SecretRotated(ref_path=path, version_token=token, rotated_at=utcnow())


class _ScriptedPubSub:
    """Replays a fixed message sequence through the query-port surface."""

    def __init__(self, messages: Sequence[PubSubMessage[object]]) -> None:
        self._messages = list(messages)

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[PubSubMessage[object]]:
        for message in self._messages:
            if message.topic in topics:
                yield message


def _message(payload: object) -> PubSubMessage[object]:
    return PubSubMessage(topic=DEFAULT_SECRET_ROTATIONS_CHANNEL, payload=payload)


class TestPubSubSource:
    async def test_rotation_events_become_changes(self) -> None:
        source = PubSubSecretsChangeSource(
            query=_ScriptedPubSub([_message(_rotated("db/dsn"))])  # type: ignore[arg-type]
        )

        seen = [change async for change in source.subscribe()]

        assert seen == [
            SecretChanged(ref=SecretRef("db/dsn"), version=seen[0].version),
        ]
        assert seen[0].version.token == "v7"

    async def test_foreign_payloads_are_ignored(self) -> None:
        source = PubSubSecretsChangeSource(
            query=_ScriptedPubSub(  # type: ignore[arg-type]
                [_message({"not": "a rotation"}), _message(_rotated("db/dsn"))]
            )
        )

        seen = [change async for change in source.subscribe()]

        assert [change.ref.path for change in seen] == ["db/dsn"]

    async def test_ref_filter_scopes_delivery(self) -> None:
        source = PubSubSecretsChangeSource(
            query=_ScriptedPubSub(  # type: ignore[arg-type]
                [_message(_rotated("db/dsn")), _message(_rotated("api/key"))]
            )
        )

        seen = [change async for change in source.subscribe(refs=(SecretRef("api/key"),))]

        assert [change.ref.path for change in seen] == ["api/key"]


class TestOutboxToPubSubChain:
    async def test_staged_event_reaches_a_live_subscriber(self) -> None:
        """The full fan-out half: outbox stage → relay to pub/sub → source yields."""

        ctx = context_from_modules(MockDepsModule())
        outbox_spec = secret_rotated_outbox_spec()
        pubsub_spec = secret_rotated_pubsub_spec()

        query = ctx.deps.resolve_configurable(
            ctx, PubSubQueryDepKey, pubsub_spec, route=pubsub_spec.name
        )
        source = PubSubSecretsChangeSource(query=query)
        seen: list[SecretChanged] = []

        async def _drain() -> None:
            async for change in source.subscribe():
                seen.append(change)
                return

        task = asyncio.create_task(_drain())
        await asyncio.sleep(0)

        try:
            await publish_secret_rotated(ctx, outbox_spec, _rotated("db/dsn", token="v9"))

            relayed = await OutboxRelay(outbox_spec=outbox_spec).to_pubsub(ctx, pubsub_spec)
            assert relayed.published == 1

            await asyncio.wait_for(task, timeout=2)

        finally:
            task.cancel()

        assert [change.ref.path for change in seen] == ["db/dsn"]
        assert seen[0].version.token == "v9"
