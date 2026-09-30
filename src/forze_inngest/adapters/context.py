"""Encode and decode Forze execution context in Inngest event payloads."""

from typing import Final, cast
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze.base.primitives import JsonDict

# ----------------------- #

_FORZE_ENVELOPE_KEY: Final[str] = "_forze"

EXEC_ID_KEY: Final[str] = "execution_id"
CORR_ID_KEY: Final[str] = "correlation_id"
CAUS_ID_KEY: Final[str] = "causation_id"
PRINCIPAL_ID_KEY: Final[str] = "principal_id"
ACTOR_IDS_KEY: Final[str] = "actor_ids"
TENANT_ID_KEY: Final[str] = "tenant_id"


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestDecodedContext:
    """Execution context fields recovered from an event payload envelope."""

    metadata: InvocationMetadata | None = None
    authn: AuthnIdentity | None = None
    tenant: TenantIdentity | None = None

    identity_malformed: bool = False
    """The envelope claims a principal, actor chain or tenant that does not decode. Only a
    function that binds the envelope's identity has to refuse it; the rest ignore it."""


# ....................... #


def merge_envelope(
    data: JsonDict,
    *,
    metadata: InvocationMetadata | None = None,
    authn: AuthnIdentity | None = None,
    tenant: TenantIdentity | None = None,
) -> JsonDict:
    """Attach a Forze context envelope under ``_forze`` when any field is set."""

    envelope: JsonDict = {}

    if metadata is not None:
        envelope[EXEC_ID_KEY] = str(metadata.execution_id)
        envelope[CORR_ID_KEY] = str(metadata.correlation_id)

        if metadata.causation_id is not None:
            envelope[CAUS_ID_KEY] = str(metadata.causation_id)

    if authn is not None:
        envelope[PRINCIPAL_ID_KEY] = str(authn.principal_id)

        # Dropping the actor would run an agent's event as the user alone.
        actor_ids: list[str] = []
        actor = authn.actor

        while actor is not None:
            actor_ids.append(str(actor.principal_id))
            actor = actor.actor

        if actor_ids:
            envelope[ACTOR_IDS_KEY] = actor_ids

    if tenant is not None:
        envelope[TENANT_ID_KEY] = str(tenant.tenant_id)

    if not envelope:
        return data

    merged = dict(data)
    merged[_FORZE_ENVELOPE_KEY] = envelope
    return merged


# ....................... #


def split_envelope(data: JsonDict) -> tuple[InngestDecodedContext, JsonDict]:
    """Split ``_forze`` envelope from business payload data."""

    raw = data.get(_FORZE_ENVELOPE_KEY)

    if not isinstance(raw, dict):
        return InngestDecodedContext(), data

    raw = cast(JsonDict, raw)

    payload = {k: v for k, v in data.items() if k != _FORZE_ENVELOPE_KEY}

    # The envelope is producer-controlled text, so each part decodes on its own: a malformed
    # part must not stop a function that never reads it.
    metadata = None
    authn = None
    tenant = None
    identity_malformed = False

    try:
        if exec_raw := raw.get(EXEC_ID_KEY):
            corr_raw = raw.get(CORR_ID_KEY) or exec_raw
            caus_raw = raw.get(CAUS_ID_KEY)

            metadata = InvocationMetadata(
                execution_id=UUID(str(exec_raw)),
                correlation_id=UUID(str(corr_raw)),
                causation_id=UUID(str(caus_raw)) if caus_raw else None,
            )

    except ValueError:
        # Tracing context, not an authority: dropped rather than trusted or fatal.
        metadata = None

    # A claimed part is any key present with a value; one that does not decode marks the
    # identity malformed. The principal chain and the tenant decode apart, because a sealed
    # payload's AAD needs the tenant even where the identity is not bound.
    principal_raw = raw.get(PRINCIPAL_ID_KEY)
    actor_ids = raw.get(ACTOR_IDS_KEY)

    try:
        if actor_ids is not None and (principal_raw is None or not isinstance(actor_ids, list)):
            raise ValueError("actor_ids needs a principal and must be a list")

        if principal_raw is not None:
            actor = None

            for actor_raw in reversed(cast(list[object], actor_ids or [])):
                actor = AuthnIdentity(principal_id=UUID(str(actor_raw)), actor=actor)

            authn = AuthnIdentity(principal_id=UUID(str(principal_raw)), actor=actor)

    except ValueError:
        authn, identity_malformed = None, True

    try:
        if (tenant_raw := raw.get(TENANT_ID_KEY)) is not None:
            tenant = TenantIdentity(tenant_id=UUID(str(tenant_raw)))

    except ValueError:
        tenant, identity_malformed = None, True

    return (
        InngestDecodedContext(
            metadata=metadata,
            authn=authn,
            tenant=tenant,
            identity_malformed=identity_malformed,
        ),
        payload,
    )


# ....................... #


def parse_function_args[In: BaseModel](
    data: JsonDict,
    *,
    args_type: type[In],
) -> In:
    """Validate function arguments from event ``data`` after removing the envelope."""

    _, payload = split_envelope(data)
    return args_type.model_validate(payload)
