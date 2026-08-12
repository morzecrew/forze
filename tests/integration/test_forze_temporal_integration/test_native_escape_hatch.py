"""The footgun test: ``native`` carries the framework client's codec, against a real server.

The bug class this pins is not a crash — it is silence. An application needing SDK surface
the port omits used to build a second ``Client.connect`` by hand; that client has no
``EncryptingPayloadCodec``, so every payload it writes lands in the Temporal datastore in
**plaintext** while the deployment believes durable payloads are sealed. Nothing fails, no
test goes red, and the port still reads the run back fine — the codec passes unrecognized
payloads through untouched by design.

So round-tripping alone cannot detect it. What can: reading the same run's history with a
*plain* client and asserting the bytes at rest are sealed.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import Keyring
from forze_mock import MockKeyManagement
from forze_temporal import encrypting_data_converter, sandboxed_workflow_runner
from forze_temporal.kernel.client import TemporalClient, TemporalConfig

from ._workflow_defs import EchoIn, EchoOut, ItEchoWorkflow

# ----------------------- #

_SEALED_ENCODING = b"binary/forze-encrypted"
"""What :class:`~forze_temporal.EncryptingPayloadCodec` stamps on a payload it sealed."""


def _payload_encodings(events) -> set[bytes]:
    """Every ``encoding`` metadata value on any payload in *events*."""

    encodings: set[bytes] = set()

    for event in events:
        for payloads in _payload_sets(event):
            for payload in payloads:
                encodings.add(payload.metadata.get("encoding", b""))

    return encodings


def _payload_sets(event):
    """The argument and result payload sets a history event can carry."""

    if event.HasField("workflow_execution_started_event_attributes"):
        yield event.workflow_execution_started_event_attributes.input.payloads

    if event.HasField("workflow_execution_completed_event_attributes"):
        yield event.workflow_execution_completed_event_attributes.result.payloads


# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_native_writes_are_sealed_and_read_back_through_the_port(
    temporal_dev_target,
) -> None:
    """A run started through the escape hatch is sealed at rest and readable via the port.

    Both halves matter. The port read proves ``native`` is the *same* connection (a second
    client would seal under no key at all, and the two would still agree — see the second
    assertion for why that is not enough). The plaintext-absence check is the one that
    actually fails when the hatch stops sharing the codec.
    """

    keyring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )
    forze_client = TemporalClient()
    await forze_client.initialize(
        temporal_dev_target.grpc_address,
        config=TemporalConfig(
            namespace="default",
            data_converter=encrypting_data_converter(keyring),
        ),
    )

    marker = f"plaintext-marker-{uuid4()}"
    workflow_id = f"native-hatch-{uuid4()}"
    task_queue = f"native-hatch-tq-{uuid4()}"

    try:
        # The worker itself is built from the hatch — one connection, codec included.
        async with Worker(
            forze_client.native,
            task_queue=task_queue,
            workflows=[ItEchoWorkflow],
            workflow_runner=sandboxed_workflow_runner(),
        ):
            await forze_client.native.start_workflow(
                ItEchoWorkflow.run,
                EchoIn(marker=marker),
                id=workflow_id,
                task_queue=task_queue,
            )

            # Written via ``native``, read via the port: the round trip the RFC asks for.
            result = await forze_client.get_workflow_result(
                workflow_id,
                result_type=EchoOut,
            )

        assert EchoOut.model_validate(result).echoed == marker

        # At rest, through a client with no codec of its own: sealed, and the marker gone.
        plain = await Client.connect(
            temporal_dev_target.grpc_address,
            data_converter=pydantic_data_converter,
        )
        history = await plain.get_workflow_handle(workflow_id).fetch_history()

        assert _payload_encodings(history.events) == {_SEALED_ENCODING}
        assert marker.encode() not in history.to_json().encode()

    finally:
        await forze_client.close()
