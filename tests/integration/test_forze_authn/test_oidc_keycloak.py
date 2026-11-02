"""RFC 0009 P1 against a real Keycloak: discovery → JWKS → claims, and RFC 7662 introspection.

One realm, three clients: ``app`` mints client-credentials tokens (audience ``orders-api``,
realm role ``billing-admin``, client role ``reader``, group ``/ops``); ``orders-api`` is the
resource server introspecting with ``client_secret_basic``; ``orders-api-jwt`` introspects
with ``private_key_jwt``. The mock transports of the unit tier prove the verifiers' logic;
this proves the shapes they assume are Keycloak's.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import httpx
import pytest

pytest.importorskip("jwt")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from pydantic import SecretStr

from forze.application.contracts.authn import AccessTokenCredentials, AuthnSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.oidc import (
    FORZE_CLAIMS,
    BasicClientAuth,
    ClaimsPath,
    ConfigurableOidcIdpVerifier,
    IntrospectionConfig,
    IntrospectionTokenVerifier,
    OidcClaimMapper,
    OidcIdpPreset,
    PrivateKeyJwtAuth,
)
from tests.support.keycloak import keycloak_realm

pytestmark = pytest.mark.integration

REALM = "forze"
RESOURCE = "orders-api"
SPEC = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))
MAPPER = OidcClaimMapper(
    email_claim="email",
    roles=ClaimsPath("realm_access.roles"),
    groups=ClaimsPath("groups"),
)
CLIENT_ROLES = ClaimsPath("resource_access.{client}.roles", client=RESOURCE)

# ----------------------- #


@dataclass(frozen=True, slots=True)
class Realm:
    issuer: str
    app_secret: str
    rs_secret: str
    rs_key_pem: str

    def token(self) -> str:
        r = httpx.post(
            f"{self.issuer}/protocol/openid-connect/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "app",
                "client_secret": self.app_secret,
            },
            timeout=30,
        )
        r.raise_for_status()
        return str(r.json()["access_token"])

    def revoke(self, token: str) -> None:
        r = httpx.post(
            f"{self.issuer}/protocol/openid-connect/revoke",
            data={"token": token, "client_id": "app", "client_secret": self.app_secret},
            timeout=30,
        )
        r.raise_for_status()


def _confidential(client_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "clientId": client_id,
        "enabled": True,
        "publicClient": False,
        "standardFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": False,
        **extra,
    }


def _realm(jwks: dict[str, Any]) -> dict[str, Any]:
    in_tokens = {"access.token.claim": "true", "introspection.token.claim": "true"}
    return {
        "realm": REALM,
        "enabled": True,
        "roles": {
            "realm": [{"name": "billing-admin"}],
            "client": {RESOURCE: [{"name": "reader"}]},
        },
        "groups": [{"name": "ops", "path": "/ops"}],
        "clients": [
            _confidential(RESOURCE, secret="${RS_SECRET}", clientAuthenticatorType="client-secret"),
            _confidential(
                f"{RESOURCE}-jwt",
                clientAuthenticatorType="client-jwt",
                attributes={"use.jwks.string": "true", "jwks.string": json.dumps(jwks)},
            ),
            _confidential(
                "app",
                secret="${APP_SECRET}",
                clientAuthenticatorType="client-secret",
                serviceAccountsEnabled=True,
                protocolMappers=[
                    *(
                        {
                            "name": f"audience-{audience}",
                            "protocol": "openid-connect",
                            "protocolMapper": "oidc-audience-mapper",
                            "config": {"included.client.audience": audience, **in_tokens},
                        }
                        # Keycloak answers active:false to an introspecting client that is
                        # not in the token's audience, so each resource server is in it.
                        for audience in (RESOURCE, f"{RESOURCE}-jwt")
                    ),
                    {
                        "name": "groups",
                        "protocol": "openid-connect",
                        "protocolMapper": "oidc-group-membership-mapper",
                        "config": {"claim.name": "groups", "full.path": "true", **in_tokens},
                    },
                ],
            ),
        ],
        "users": [
            {
                "username": "service-account-app",
                "enabled": True,
                "serviceAccountClientId": "app",
                "email": "app@example.com",
                "emailVerified": True,
                "realmRoles": ["billing-admin"],
                "clientRoles": {RESOURCE: ["reader"]},
                "groups": ["/ops"],
            }
        ],
    }


@pytest.fixture(scope="module")
def realm() -> Iterator[Realm]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwks = {"keys": [{**jwk, "kid": "rs-key", "use": "sig", "alg": "RS256"}]}
    app_secret, rs_secret = secrets.token_hex(16), secrets.token_hex(16)

    with keycloak_realm(
        _realm(jwks),
        env={"APP_SECRET": app_secret, "RS_SECRET": rs_secret},
    ) as issuer:
        yield Realm(
            issuer=issuer,
            app_secret=app_secret,
            rs_secret=rs_secret,
            rs_key_pem=key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ).decode(),
        )


def _assert_claims(claims: dict[str, Any]) -> None:
    normalized = claims[FORZE_CLAIMS]
    assert normalized["email"] == "app@example.com"
    assert "billing-admin" in normalized["roles"]
    assert normalized["groups"] == ["/ops"]
    assert CLIENT_ROLES.extract(claims) == ("reader",)


# ----------------------- #


async def test_discovery_jwks_and_claims(realm: Realm) -> None:
    preset = await OidcIdpPreset.from_issuer(realm.issuer, audience=RESOURCE, claim_mapper=MAPPER)

    assert preset.metadata is not None
    assert preset.metadata.introspection_endpoint is not None

    verifier = ConfigurableOidcIdpVerifier(preset=preset)(None, SPEC)  # type: ignore[arg-type]
    assertion = await verifier.verify_token(AccessTokenCredentials(token=realm.token()))

    assert assertion.issuer == realm.issuer
    assert assertion.audience == RESOURCE
    _assert_claims(dict(assertion.claims))


async def test_discovery_refuses_a_wrong_issuer(realm: Realm) -> None:
    # Same document, asked for under a spelling Keycloak does not use for itself.
    with pytest.raises(CoreException) as caught:
        await OidcIdpPreset.from_issuer(realm.issuer + "/", audience=RESOURCE)

    assert caught.value.code == "oidc_issuer_mismatch"


async def _introspection(realm: Realm, client_auth: Any) -> IntrospectionTokenVerifier:
    preset = await OidcIdpPreset.from_issuer(realm.issuer, audience=RESOURCE)
    assert preset.metadata is not None and preset.metadata.introspection_endpoint is not None

    return IntrospectionTokenVerifier(
        config=IntrospectionConfig(
            endpoint=preset.metadata.introspection_endpoint,
            issuer=realm.issuer,
            audience=RESOURCE,
            client_auth=client_auth,
            claim_mapper=MAPPER,
            cache_ttl=timedelta(0),
        ),
    )


async def test_introspection_basic_auth_accepts_and_revocation_rejects(realm: Realm) -> None:
    verifier = await _introspection(
        realm,
        BasicClientAuth(client_id=RESOURCE, client_secret=SecretStr(realm.rs_secret)),
    )
    token = realm.token()

    assertion = await verifier.verify_token(AccessTokenCredentials(token=token))

    assert assertion.issuer == realm.issuer
    assert assertion.audience == RESOURCE
    _assert_claims(dict(assertion.claims))

    realm.revoke(token)

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(AccessTokenCredentials(token=token))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


async def test_introspection_rejects_garbage(realm: Realm) -> None:
    verifier = await _introspection(
        realm,
        BasicClientAuth(client_id=RESOURCE, client_secret=realm.rs_secret),
    )

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(AccessTokenCredentials(token=secrets.token_urlsafe(32)))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


async def test_introspection_with_wrong_client_secret_fails_closed(realm: Realm) -> None:
    verifier = await _introspection(
        realm,
        BasicClientAuth(client_id=RESOURCE, client_secret=secrets.token_hex(16)),
    )

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(AccessTokenCredentials(token=realm.token()))

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    # The refusal is about this client, not the token (RFC 7662 §2.3).
    assert caught.value.details == {"status": 401}


async def test_introspection_private_key_jwt(realm: Realm) -> None:
    verifier = await _introspection(
        realm,
        PrivateKeyJwtAuth(
            client_id=f"{RESOURCE}-jwt",
            private_key=SecretStr(realm.rs_key_pem),
            audience=realm.issuer,
            key_id="rs-key",
        ),
    )

    assertion = await verifier.verify_token(AccessTokenCredentials(token=realm.token()))

    _assert_claims(dict(assertion.claims))
