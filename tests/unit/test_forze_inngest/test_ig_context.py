from uuid import uuid4

from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze_inngest.adapters.context import (
    merge_envelope,
    parse_function_args,
    split_envelope,
)


class _Args(BaseModel):
    value: str


def test_merge_and_split_envelope_round_trip() -> None:
    metadata = InvocationMetadata(
        execution_id=uuid4(),
        correlation_id=uuid4(),
    )
    authn = AuthnIdentity(principal_id=uuid4())
    tenant = TenantIdentity(tenant_id=uuid4())

    data = merge_envelope(
        {"value": "ok"},
        metadata=metadata,
        authn=authn,
        tenant=tenant,
    )

    decoded, payload = split_envelope(data)

    assert payload == {"value": "ok"}
    assert decoded.metadata == metadata
    assert decoded.authn == authn
    assert decoded.tenant == tenant

    args = parse_function_args(data, args_type=_Args)
    assert args.value == "ok"
