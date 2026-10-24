"""Recipe: run an application's operations from the command line with ``forze ops``.

An ``OpsApp`` declares what the CLI exposes, the runtime a call runs on, and who the call
runs as. Each ``forze ops call`` then validates the input, verifies the API key, resolves the
principal's tenant and runs the operation through the normal pipeline — authz, tenancy,
read-only enforcement, idempotency and audit apply exactly as over HTTP. No ad-hoc script
holding database credentials, and every call leaves the same trail an HTTP call would.

Run it (the in-memory mock keeps no state between processes, so each call starts empty)::

    export FORZE_OPS_APP=examples.recipes.ops_cli.app:ops
    forze ops list
    forze ops describe notes.create
    FORZE_API_KEY=demo-operator-key forze ops call notes.create --input '{"title": "hi"}'

Exercised by tests/unit/test_examples/test_ops_cli.py.
"""

from __future__ import annotations

# --8<-- [start:declaration]
from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import DeploymentProfile, ExecutionRuntime, build_runtime
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_cli.ops import ApiKeyIdentity, OpsApp
from forze_kits.aggregates.document.factories import build_document_registry
from forze_mock import MockDepsModule, MockState

# --8<-- [end:declaration]

# ----------------------- #


class Note(Document):
    title: str


class NoteRead(ReadDocument):
    title: str


class NoteInput(BaseDTO):
    title: str


SPEC = DocumentSpec(
    name="notes",
    read=NoteRead,
    write=DocumentWriteTypes(domain=Note, create_cmd=NoteInput),
)

STATE = MockState()

# The mock's own API-key table, so the demo key verifies. A real deployment issues keys
# through forze_identity (``issue_api_key``), one per operator or job.
STATE.identity["authn"] = {"main": {"api_keys": {"demo-operator-key": {"subject": "operator"}}}}

# --8<-- [start:runtime]


def build_ops_runtime() -> ExecutionRuntime:
    """The runtime one ``forze ops call`` runs on, built fresh per call.

    ``SERVERLESS``: assembly refuses a lifecycle step declared ``requires_long_running``,
    and the drain window is zero, so a one-shot process never starts a poller it would
    abandon. Wire only the deps the exposed operations need.
    """

    return build_runtime(MockDepsModule(state=STATE), deployment=DeploymentProfile.SERVERLESS)


# --8<-- [end:runtime]

# --8<-- [start:ops]
ops = OpsApp(
    registry=build_document_registry(SPEC).freeze(),
    runtime=build_ops_runtime,
    # Who each call runs as: the principal a key from $FORZE_API_KEY authenticates.
    identity=ApiKeyIdentity(AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))),
    include_writes=True,
    # With writes on, name what is exposed: an allowlist is reviewed like a grant.
    operations=["notes.create", "notes.get", "notes.list"],
)
# --8<-- [end:ops]
