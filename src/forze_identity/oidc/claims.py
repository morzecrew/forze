from ._compat import require_oidc

require_oidc()

# ....................... #

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Final, Protocol, final

import attrs

from forze.application.contracts.authn import VerifiedAssertion
from forze.base.exceptions import exc

# ----------------------- #

FORZE_CLAIMS: Final[str] = "_forze"
"""Key in :attr:`VerifiedAssertion.claims` holding the mapper's normalized values.

A mapping with ``email`` / ``roles`` / ``groups`` — only the ones the mapper is configured to
extract. The mapper always writes it itself: a ``_forze`` claim the issuer sent is dropped, so
a claim a user can edit at the IdP cannot pose as normalized roles."""

ZITADEL_PROJECT_ROLES_CLAIM: Final[str] = "urn:zitadel:iam:org:project:roles"
"""Zitadel's roles claim for the projects in the token's audience."""

# ....................... #


def _resolve_path(claims: Mapping[str, Any], segments: tuple[str, ...]) -> Any:
    """The value at *segments* inside nested mappings, or ``None`` when any step is missing."""

    value: Any = claims

    for segment in segments:
        if not isinstance(value, Mapping):
            return None

        value = value.get(segment)  # pyright: ignore[reportUnknownMemberType]

    return value


# ....................... #


def _split_path(path: str, *, params: Mapping[str, str], owner: str) -> tuple[str, ...]:
    """Split a dotted path, then fill ``{name}`` placeholders segment by segment.

    Filling after the split keeps a parameter value that contains dots (a Keycloak client id,
    a namespaced claim URL) as one segment.
    """

    segments: list[str] = []

    for raw in path.split("."):
        try:
            segment = raw.format_map(params)

        except (KeyError, IndexError, ValueError) as e:
            raise exc.configuration(
                f"{owner} path {path!r} has a placeholder with no value: {e}",
            ) from e

        if not segment:
            raise exc.configuration(f"{owner} path {path!r} has an empty segment")

        segments.append(segment)

    return tuple(segments)


# ....................... #


class ClaimsExtractor(Protocol):
    """Read a list of names (roles, groups) out of a verified claims payload.

    Implementations must not raise: a missing or malformed claim yields an empty tuple, so a
    surprising token shape costs the caller permissions, never its authentication.
    """

    def extract(self, claims: Mapping[str, Any]) -> tuple[str, ...]:
        """Non-empty, de-duplicated names in claim order; empty when the claim is unusable."""

        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True, init=False)
class ClaimsPath(ClaimsExtractor):
    """A dotted path to a list of names: ``ClaimsPath("realm_access.roles")``.

    ``{name}`` placeholders are filled from keyword arguments after the path is split, so
    ``ClaimsPath("resource_access.{client}.roles", client="my.app")`` reads Keycloak client
    roles even when the client id contains a dot, and ``ClaimsPath("{c}", c="https://x/roles")``
    reads a namespaced top-level claim.

    ``item_key`` reads lists of objects: Casdoor's default JWT carries ``roles`` as role
    objects, so ``ClaimsPath("roles", item_key="name")`` takes each object's ``name``. String
    items are taken as they are either way; anything else is skipped.
    """

    segments: tuple[str, ...]
    """The path, split into the claim keys walked in order."""

    item_key: str | None
    """Key read from each object item, when the list holds objects."""

    # ....................... #

    def __init__(self, path: str, /, *, item_key: str | None = None, **params: str) -> None:
        if item_key is not None and not item_key:
            raise exc.configuration("ClaimsPath item_key must be non-empty when set")

        self.__attrs_init__(  # pyright: ignore[reportAttributeAccessIssue]
            segments=_split_path(path, params=params, owner="ClaimsPath"),
            item_key=item_key,
        )

    # ....................... #

    def extract(self, claims: Mapping[str, Any]) -> tuple[str, ...]:
        value = _resolve_path(claims, self.segments)

        if not isinstance(value, (list, tuple)):
            return ()

        names: list[str] = []

        for item in value:  # pyright: ignore[reportUnknownVariableType]
            if self.item_key is not None and isinstance(item, Mapping):
                item = item.get(self.item_key)  # pyright: ignore[reportUnknownMemberType]

            if isinstance(item, str) and item:
                names.append(item)

        return tuple(dict.fromkeys(names))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ZitadelProjectRoles(ClaimsExtractor):
    """Zitadel's role-keyed org map: ``{role_key: {org_id: org_domain}}``.

    Reads :data:`ZITADEL_PROJECT_ROLES_CLAIM`, or the project-specific
    ``urn:zitadel:iam:org:project:{project_id}:roles`` claim when ``project_id`` is set. With
    ``org_id`` set, only roles granted by that organization are kept; without it, roles from
    every organization in the claim are merged, so a multi-organization deployment should set
    it.
    """

    project_id: str | None = None
    """Read the project-specific roles claim instead of the audience-wide one."""

    org_id: str | None = None
    """Keep only the roles this organization granted."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.project_id is not None and not self.project_id.strip():
            raise exc.configuration("ZitadelProjectRoles project_id must be non-empty when set")

        if self.org_id is not None and not self.org_id.strip():
            raise exc.configuration("ZitadelProjectRoles org_id must be non-empty when set")

    # ....................... #

    def extract(self, claims: Mapping[str, Any]) -> tuple[str, ...]:
        name = (
            ZITADEL_PROJECT_ROLES_CLAIM
            if self.project_id is None
            else f"urn:zitadel:iam:org:project:{self.project_id}:roles"
        )
        grants = claims.get(name)

        if not isinstance(grants, Mapping):
            return ()

        return tuple(
            role
            for role, orgs in grants.items()  # pyright: ignore[reportUnknownVariableType]
            if isinstance(role, str)
            and role
            and isinstance(orgs, Mapping)
            and (self.org_id is None or self.org_id in orgs)
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OidcClaimMapper:
    """Map a verified OIDC token's claim payload onto a :class:`VerifiedAssertion`.

    Defaults follow the OIDC core spec (``iss`` / ``sub`` / ``aud`` / ``iat`` / ``exp``).
    Override the claim names when an IdP uses non-standard keys (e.g. Firebase puts the
    tenant id under ``firebase.tenant``).

    ``email_claim``, ``roles`` and ``groups`` opt into normalization: what they extract lands
    under :data:`FORZE_CLAIMS` in the assertion's claims, under the same keys for every IdP.
    All three are off by default.
    """

    issuer_claim: str = "iss"
    """The issuer claim to use."""

    subject_claim: str = "sub"
    """The subject claim to use."""

    audience_claim: str | None = "aud"
    """The audience claim to use."""

    issued_at_claim: str | None = "iat"
    """The issued-at claim to use."""

    expires_at_claim: str | None = "exp"
    """The expiry claim to use."""

    tenant_claim: str | None = None
    """When set, the resolver picks tenant context from this claim instead of leaving it ``None``."""

    email_claim: str | None = None
    """Dotted path to the email, normalized as ``_forze.email`` (``"email"``; ``"traits.email"``
    for a Kratos session). Not proof of ownership: do not link accounts on it unless the IdP
    also asserts the address is verified."""

    roles: ClaimsExtractor | None = None
    """Extractor for role names, normalized as ``_forze.roles``."""

    groups: ClaimsExtractor | None = None
    """Extractor for group names, normalized as ``_forze.groups``."""

    _email_segments: tuple[str, ...] | None = attrs.field(
        init=False,
        repr=False,
        eq=False,
        default=attrs.Factory(
            lambda self: (
                None
                if self.email_claim is None
                else _split_path(self.email_claim, params={}, owner="email_claim")
            ),
            takes_self=True,
        ),
    )
    """:attr:`email_claim` split once at construction, which also refuses an empty segment."""

    # ....................... #

    def map(
        self,
        claims: Mapping[str, Any],
        *,
        validated_audience: str | None = None,
    ) -> VerifiedAssertion:
        issuer_raw = claims.get(self.issuer_claim)
        subject_raw = claims.get(self.subject_claim)

        if not isinstance(issuer_raw, str) or not isinstance(subject_raw, str):
            raise ValueError(
                f"OIDC claims missing required '{self.issuer_claim}' or '{self.subject_claim}'",
            )

        if not issuer_raw.strip() or not subject_raw.strip():
            raise ValueError(
                f"OIDC claims '{self.issuer_claim}' and '{self.subject_claim}' must be non-empty",
            )

        audience: str | None = None

        if validated_audience is not None:
            # The verifier passes the ``aud`` entry it actually validated against its
            # configured audience — record *that*, not an arbitrary first element of a
            # multi-audience token (which may be a different party's audience).
            audience = validated_audience

        elif self.audience_claim is not None:
            aud_raw = claims.get(self.audience_claim)

            if isinstance(aud_raw, str):
                audience = aud_raw

            elif isinstance(aud_raw, list) and aud_raw and isinstance(aud_raw[0], str):
                # No audience was enforced (mapper used standalone); OIDC permits aud as
                # an array, so fall back to the first string entry.
                audience = aud_raw[0]

        issuer_tenant_hint: str | None = None

        if self.tenant_claim is not None:
            tid_raw = claims.get(self.tenant_claim)

            if isinstance(tid_raw, str):
                issuer_tenant_hint = tid_raw

        return VerifiedAssertion(
            issuer=issuer_raw,
            subject=subject_raw,
            audience=audience,
            issuer_tenant_hint=issuer_tenant_hint,
            issued_at=self._coerce_timestamp(claims, self.issued_at_claim),
            expires_at=self._coerce_timestamp(claims, self.expires_at_claim),
            claims=self._snapshot(claims),
        )

    # ....................... #

    def _snapshot(self, claims: Mapping[str, Any]) -> dict[str, Any]:
        """The claims with the issuer's own ``_forze`` replaced by the normalized values."""

        normalized: dict[str, Any] = {}

        if self._email_segments is not None:
            email = _resolve_path(claims, self._email_segments)

            if isinstance(email, str) and email.strip():
                normalized["email"] = email

        if self.roles is not None:
            normalized["roles"] = list(self.roles.extract(claims))

        if self.groups is not None:
            normalized["groups"] = list(self.groups.extract(claims))

        snapshot = {k: v for k, v in claims.items() if k != FORZE_CLAIMS}

        if normalized:
            snapshot[FORZE_CLAIMS] = normalized

        return snapshot

    # ....................... #
    #! support for strings (?)

    @staticmethod
    def _coerce_timestamp(
        claims: Mapping[str, Any],
        name: str | None,
    ) -> datetime | None:
        """Coerce timestamp claim to a :class:`datetime` if present."""

        if name is None:
            return None

        raw = claims.get(name)

        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=UTC)

        return None
