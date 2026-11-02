"""Claims extraction tables: per-IdP role/group shapes, and malformed input never raises."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("jwt")

from forze.base.exceptions import CoreException
from forze_identity.oidc import (
    FORZE_CLAIMS,
    ClaimsExtractor,
    ClaimsPath,
    OidcClaimMapper,
    ZitadelProjectRoles,
)

pytestmark = pytest.mark.unit

# ----------------------- #

_KEYCLOAK: dict[str, Any] = {
    "iss": "https://kc.example/realms/acme",
    "sub": "u-1",
    "realm_access": {"roles": ["billing-admin", "offline_access", "billing-admin"]},
    "resource_access": {
        "orders-api": {"roles": ["reader"]},
        "my.app": {"roles": ["writer"]},
    },
    "groups": ["/ops", "/ops/oncall"],
}

_ZITADEL_ROLES = {
    "admin": {"org-1": "acme.zitadel.cloud"},
    "viewer": {"org-1": "acme.zitadel.cloud", "org-2": "globex.zitadel.cloud"},
    "auditor": {"org-2": "globex.zitadel.cloud"},
}

_ZITADEL: dict[str, Any] = {
    "iss": "https://acme.zitadel.cloud",
    "sub": "u-2",
    "urn:zitadel:iam:org:project:roles": _ZITADEL_ROLES,
    "urn:zitadel:iam:org:project:p-9:roles": {"ops": {"org-1": "acme.zitadel.cloud"}},
}

# Casdoor's default JWT: ``roles`` / ``permissions`` are objects, ``groups`` strings.
_CASDOOR: dict[str, Any] = {
    "iss": "https://door.example",
    "sub": "u-3",
    "roles": [
        {"owner": "acme", "name": "role-admin", "displayName": "Admin"},
        {"owner": "acme", "name": "role-viewer", "displayName": "Viewer"},
    ],
    "groups": ["acme/staff"],
}


@pytest.mark.parametrize(
    ("extractor", "claims", "expected"),
    [
        pytest.param(
            ClaimsPath("realm_access.roles"),
            _KEYCLOAK,
            ("billing-admin", "offline_access"),
            id="keycloak-realm-roles-deduplicated",
        ),
        pytest.param(
            ClaimsPath("resource_access.{client}.roles", client="orders-api"),
            _KEYCLOAK,
            ("reader",),
            id="keycloak-client-roles",
        ),
        pytest.param(
            ClaimsPath("resource_access.{client}.roles", client="my.app"),
            _KEYCLOAK,
            ("writer",),
            id="keycloak-client-id-with-dot-is-one-segment",
        ),
        pytest.param(ClaimsPath("groups"), _KEYCLOAK, ("/ops", "/ops/oncall"), id="kc-groups"),
        pytest.param(
            ZitadelProjectRoles(),
            _ZITADEL,
            ("admin", "viewer", "auditor"),
            id="zitadel-multi-org-unfiltered",
        ),
        pytest.param(
            ZitadelProjectRoles(org_id="org-1"),
            _ZITADEL,
            ("admin", "viewer"),
            id="zitadel-org-filtered",
        ),
        pytest.param(
            ZitadelProjectRoles(org_id="org-2"),
            _ZITADEL,
            ("viewer", "auditor"),
            id="zitadel-other-org-filtered",
        ),
        pytest.param(ZitadelProjectRoles(org_id="org-404"), _ZITADEL, (), id="zitadel-unknown-org"),
        pytest.param(
            ZitadelProjectRoles(project_id="p-9"), _ZITADEL, ("ops",), id="zitadel-project-claim"
        ),
        pytest.param(
            ClaimsPath("roles", item_key="name"),
            _CASDOOR,
            ("role-admin", "role-viewer"),
            id="casdoor-role-objects",
        ),
        pytest.param(ClaimsPath("groups"), _CASDOOR, ("acme/staff",), id="casdoor-groups"),
        pytest.param(
            ClaimsPath("{c}", c="https://app.example/roles"),
            {"https://app.example/roles": ["a"]},
            ("a",),
            id="namespaced-claim-with-dots",
        ),
    ],
)
def test_extraction_table(
    extractor: ClaimsExtractor,
    claims: dict[str, Any],
    expected: tuple[str, ...],
) -> None:
    assert extractor.extract(claims) == expected


@pytest.mark.parametrize(
    "claims",
    [
        pytest.param({}, id="missing"),
        pytest.param({"realm_access": None}, id="null-parent"),
        pytest.param({"realm_access": "admin"}, id="string-parent"),
        pytest.param({"realm_access": {"roles": "admin"}}, id="string-not-list"),
        pytest.param({"realm_access": {"roles": {"admin": True}}}, id="dict-not-list"),
        pytest.param({"realm_access": {"roles": 7}}, id="number"),
        pytest.param({"realm_access": {"roles": [1, None, "", {"x": 1}, ["a"]]}}, id="junk-items"),
        pytest.param({"realm_access": [{"roles": ["a"]}]}, id="list-parent"),
    ],
)
def test_claims_path_malformed_is_empty_never_raises(claims: dict[str, Any]) -> None:
    assert ClaimsPath("realm_access.roles").extract(claims) == ()


@pytest.mark.parametrize(
    "grants",
    [
        pytest.param(None, id="missing"),
        pytest.param(["admin"], id="list-not-map"),
        pytest.param("admin", id="string"),
        pytest.param({"admin": ["org-1"]}, id="org-list-not-map"),
        pytest.param({"admin": None}, id="org-null"),
        pytest.param({"": {"org-1": "d"}}, id="empty-role-key"),
    ],
)
def test_zitadel_malformed_is_empty_never_raises(grants: object) -> None:
    claims = {} if grants is None else {"urn:zitadel:iam:org:project:roles": grants}

    assert ZitadelProjectRoles().extract(claims) == ()
    assert ZitadelProjectRoles(org_id="org-1").extract(claims) == ()


def test_casdoor_objects_without_item_key_are_skipped() -> None:
    assert ClaimsPath("roles").extract(_CASDOOR) == ()


# ....................... #


class TestMapperNormalization:
    def test_off_by_default_and_snapshot_unchanged(self) -> None:
        assertion = OidcClaimMapper().map(_KEYCLOAK)

        assert FORZE_CLAIMS not in assertion.claims
        assert assertion.claims == _KEYCLOAK

    def test_keycloak_roles_groups_email(self) -> None:
        mapper = OidcClaimMapper(
            email_claim="email",
            roles=ClaimsPath("realm_access.roles"),
            groups=ClaimsPath("groups"),
        )

        assertion = mapper.map({**_KEYCLOAK, "email": "ann@example.com"})

        assert assertion.claims[FORZE_CLAIMS] == {
            "email": "ann@example.com",
            "roles": ["billing-admin", "offline_access"],
            "groups": ["/ops", "/ops/oncall"],
        }

    def test_nested_email_path(self) -> None:
        mapper = OidcClaimMapper(email_claim="traits.email")

        assertion = mapper.map({"iss": "i", "sub": "s", "traits": {"email": "k@example.com"}})

        assert assertion.claims[FORZE_CLAIMS] == {"email": "k@example.com"}

    def test_configured_but_absent_roles_are_an_empty_list(self) -> None:
        mapper = OidcClaimMapper(roles=ZitadelProjectRoles(), email_claim="email")

        assertion = mapper.map({"iss": "i", "sub": "s", "email": 42})

        assert assertion.claims[FORZE_CLAIMS] == {"roles": []}

    def test_issuer_supplied_forze_claim_is_dropped(self) -> None:
        """A ``_forze`` claim from the token must never pose as normalized roles."""

        forged = {"iss": "i", "sub": "s", FORZE_CLAIMS: {"roles": ["admin"]}}

        assert FORZE_CLAIMS not in OidcClaimMapper().map(forged).claims

        mapper = OidcClaimMapper(roles=ClaimsPath("realm_access.roles"))

        assert mapper.map(forged).claims[FORZE_CLAIMS] == {"roles": []}


# ....................... #


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda: ClaimsPath("resource_access.{client}.roles"), id="unfilled"),
        pytest.param(lambda: ClaimsPath("a..b"), id="empty-segment"),
        pytest.param(lambda: ClaimsPath(""), id="empty-path"),
        pytest.param(lambda: ClaimsPath("roles", item_key=""), id="empty-item-key"),
        pytest.param(lambda: ZitadelProjectRoles(org_id=" "), id="blank-org"),
        pytest.param(lambda: ZitadelProjectRoles(project_id=""), id="blank-project"),
        pytest.param(lambda: OidcClaimMapper(email_claim="traits."), id="bad-email-path"),
    ],
)
def test_bad_configuration_refused_at_construction(build: Any) -> None:
    with pytest.raises(CoreException):
        build()
