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
TENANT_ID_KEY: Final[str] = "tenant_id"


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestDecodedContext:
    """Execution context fields recovered from an event payload envelope."""

    metadata: InvocationMetadata | None = None
    authn: AuthnIdentity | None = None
    tenant: TenantIdentity | None = None


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

    metadata = None
    authn = None
    tenant = None

    if exec_raw := raw.get(EXEC_ID_KEY):
        corr_raw = raw.get(CORR_ID_KEY) or exec_raw
        caus_raw = raw.get(CAUS_ID_KEY)

        metadata = InvocationMetadata(
            execution_id=UUID(str(exec_raw)),
            correlation_id=UUID(str(corr_raw)),
            causation_id=UUID(str(caus_raw)) if caus_raw else None,
        )

    if principal_raw := raw.get(PRINCIPAL_ID_KEY):
        authn = AuthnIdentity(principal_id=UUID(str(principal_raw)))

    if tenant_raw := raw.get(TENANT_ID_KEY):
        tenant = TenantIdentity(tenant_id=UUID(str(tenant_raw)))

    return (
        InngestDecodedContext(metadata=metadata, authn=authn, tenant=tenant),
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
