"""Integration test: BYOK access-token signing via Vault Transit (real Vault).

The RSA private key never leaves Vault — Forze sends the signing input and gets a
signature back. Verification is local via the published public key / JWKS.
"""

from uuid import uuid4

import pytest

pytest.importorskip("jwt")

import jwt
from jwt import PyJWK

from forze_identity.authn.services import AccessTokenService, jwks_document
from forze_vault import VaultConfig, VaultTransitSigner
from forze_vault.kernel.client import VaultClient

# ----------------------- #

_KEY = "access-token-signing"
_EC_KEY = "access-token-signing-ec"


@pytest.fixture
async def transit_signer(vault_container):
    container, hvac_client = vault_container

    hvac_client.secrets.transit.create_key(
        name=_KEY,
        key_type="rsa-2048",
        mount_point="transit",
    )

    config = VaultConfig(
        url=container.get_connection_url(),
        token=container.root_token,
        transit_mount="transit",
        verify=False,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        yield VaultTransitSigner(client=client, key_name=_KEY, kid="vault-1")
    finally:
        await client.close()


@pytest.fixture
async def es256_transit_signer(vault_container):
    container, hvac_client = vault_container

    hvac_client.secrets.transit.create_key(
        name=_EC_KEY,
        key_type="ecdsa-p256",
        mount_point="transit",
    )

    config = VaultConfig(
        url=container.get_connection_url(),
        token=container.root_token,
        transit_mount="transit",
        verify=False,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        yield VaultTransitSigner(
            client=client, key_name=_EC_KEY, kid="vault-ec-1", algorithm="ES256"
        )
    finally:
        await client.close()


# ....................... #


@pytest.mark.integration
async def test_vault_signed_token_round_trip(
    transit_signer: VaultTransitSigner,
) -> None:
    svc = AccessTokenService(signer=transit_signer)
    pid = uuid4()

    token = await svc.issue_token(principal_id=pid, tenant_id=uuid4())
    claims = await svc.verify_token(token)

    assert claims["sub"] == str(pid)
    assert "tid" in claims


@pytest.mark.integration
async def test_vault_token_verifiable_via_published_jwks(
    transit_signer: VaultTransitSigner,
) -> None:
    svc = AccessTokenService(signer=transit_signer)
    token = await svc.issue_token(principal_id=uuid4())

    # An external verifier validates using only the JWKS Forze would publish.
    doc = await jwks_document(transit_signer)
    public_key = PyJWK(doc["keys"][0]).key

    claims = jwt.decode(
        token,
        key=public_key,
        algorithms=["RS256"],
        issuer="forze",
        audience="forze",
    )
    assert claims["sub"] is not None


# ....................... #
# ES256 (ECDSA P-256) — the EC private key never leaves Vault either.


@pytest.mark.integration
async def test_vault_es256_signed_token_round_trip(
    es256_transit_signer: VaultTransitSigner,
) -> None:
    svc = AccessTokenService(signer=es256_transit_signer)
    pid = uuid4()

    token = await svc.issue_token(principal_id=pid, tenant_id=uuid4())
    claims = await svc.verify_token(token)

    assert claims["sub"] == str(pid)
    assert jwt.get_unverified_header(token)["alg"] == "ES256"


@pytest.mark.integration
async def test_vault_es256_token_verifiable_via_published_jwks(
    es256_transit_signer: VaultTransitSigner,
) -> None:
    svc = AccessTokenService(signer=es256_transit_signer)
    token = await svc.issue_token(principal_id=uuid4())

    doc = await jwks_document(es256_transit_signer)
    public_key = PyJWK(doc["keys"][0]).key

    claims = jwt.decode(
        token,
        key=public_key,
        algorithms=["ES256"],
        issuer="forze",
        audience="forze",
    )
    assert claims["sub"] is not None
