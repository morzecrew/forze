"""The verified-claims cache of :class:`AccessTokenService`: what it reuses, and what it never does."""

from __future__ import annotations

import secrets
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import attrs
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from forze.application.contracts.authn import AccessTokenCredentials
from forze.base.exceptions import CoreException
from forze.base.primitives import bind_time_source
from forze_identity.authn import ForzeJwtTokenVerifier
from forze_identity.authn.services import (
    AccessTokenConfig,
    AccessTokenService,
    Hs256Signer,
    LocalAsymmetricSigner,
)

pytestmark = pytest.mark.unit

# ----------------------- #

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


@attrs.define(slots=True)
class _Clock:
    """A time source whose wall clock and monotonic clock tests move by hand."""

    wall: datetime = _T0
    mono: float = 0.0

    def now(self) -> datetime:
        return self.wall

    def uuid(self) -> UUID:
        return uuid4()

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.wall += timedelta(seconds=seconds)
        self.mono += seconds


@attrs.define(slots=True)
class _MutableSigner:
    """An HS256 signer whose key can change at runtime, as a KMS-held key's can."""

    secret: bytes
    kid: str | None = None
    algorithm: str = "HS256"

    async def sign(self, signing_input: bytes) -> bytes:
        return await Hs256Signer(secret=self.secret).sign(signing_input)

    async def verification_key(self) -> Any:
        return self.secret

    async def public_jwk(self) -> dict[str, Any] | None:
        return None


@pytest.fixture
def clock() -> Iterator[_Clock]:
    source = _Clock()

    with bind_time_source(source):
        yield source


@pytest.fixture
def decodes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every token ``jwt.decode`` is called with — one per signature verification."""

    calls: list[str] = []
    real = jwt.decode

    def counting(*args: Any, **kwargs: Any) -> Any:
        calls.append(kwargs["jwt"])
        return real(*args, **kwargs)

    monkeypatch.setattr(jwt, "decode", counting)
    return calls


def _svc(signer: Any = None, **cfg: Any) -> AccessTokenService:
    return AccessTokenService(
        signer=signer or Hs256Signer(secret=secrets.token_bytes(32)),
        config=AccessTokenConfig(issuer="it", audience="api", **cfg),
    )


async def _expect(code: str, svc: AccessTokenService, token: str, **kw: Any) -> None:
    with pytest.raises(CoreException) as ei:
        await svc.verify_token(token, **kw)

    assert ei.value.code == code


# ....................... #


async def test_a_repeated_token_is_verified_once(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc()
    token = await svc.issue_token(principal_id=uuid4())

    first = await svc.verify_token(token)
    second = await svc.verify_token(token)

    assert first == second
    assert len(decodes) == 1
    assert svc.signing_stats().verified == 2


async def test_a_different_token_misses(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc()
    a = await svc.issue_token(principal_id=uuid4())
    b = await svc.issue_token(principal_id=uuid4())

    assert (await svc.verify_token(a))["sub"] != (await svc.verify_token(b))["sub"]
    assert decodes == [a, b]


async def test_a_hit_still_expires_at_exp_less_the_leeway(
    clock: _Clock,
    decodes: list[str],
) -> None:
    svc = _svc(expires_in=timedelta(seconds=60), verify_cache_ttl=timedelta(minutes=5))
    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    # At ``exp``: inside the default 10 s leeway, but not with none.
    clock.advance(60)
    await svc.verify_token(token)
    await _expect("access_token_expired", svc, token, leeway=timedelta(0))

    # Ten seconds past it, the leeway is spent too.
    clock.advance(9)
    await svc.verify_token(token)
    clock.advance(1)
    await _expect("access_token_expired", svc, token)

    assert len(decodes) == 1, "every verdict above came from the cache"
    assert svc.signing_stats().verify_failed == 2


async def test_an_entry_expires_after_the_ttl(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc(verify_cache_ttl=timedelta(seconds=30))
    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    clock.advance(29.9)
    await svc.verify_token(token)
    assert len(decodes) == 1

    clock.advance(0.1)
    await svc.verify_token(token)
    assert len(decodes) == 2, "the TTL counts from the first verification, not the last hit"


async def test_a_changed_key_misses_and_refuses(clock: _Clock, decodes: list[str]) -> None:
    signer = _MutableSigner(secret=secrets.token_bytes(32))
    svc = _svc(signer)
    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    signer.secret = secrets.token_bytes(32)

    await _expect("invalid_access_token", svc, token)
    assert len(decodes) == 2


async def test_a_changed_algorithm_misses(clock: _Clock, decodes: list[str]) -> None:
    signer = _MutableSigner(secret=secrets.token_bytes(32))
    svc = _svc(signer)
    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    signer.algorithm = "HS512"

    await _expect("invalid_access_token", svc, token)
    assert len(decodes) == 2


@pytest.mark.parametrize("field", ["issuer", "audience"])
async def test_a_changed_issuer_or_audience_misses(
    clock: _Clock,
    decodes: list[str],
    field: str,
) -> None:
    svc = _svc()
    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    svc.config = attrs.evolve(svc.config, **{field: "other"})

    await _expect("invalid_access_token", svc, token)
    assert len(decodes) == 2


async def test_a_removed_kid_is_refused_on_a_hit(clock: _Clock, decodes: list[str]) -> None:
    signer = LocalAsymmetricSigner(
        private_key=ec.generate_private_key(ec.SECP256R1()),
        algorithm="ES256",
        kid="k1",
    )
    svc = _svc(signer)
    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)

    # An equal public key, though a fresh object on every call, is the same key: a hit.
    await svc.verify_token(token)
    assert len(decodes) == 1

    svc._verifier_by_kid.clear()  # pyright: ignore[reportPrivateUsage]

    await _expect("invalid_access_token", svc, token)


async def test_a_rotation_overlap_key_hits(clock: _Clock, decodes: list[str]) -> None:
    old = LocalAsymmetricSigner(
        private_key=ec.generate_private_key(ec.SECP256R1()),
        algorithm="ES256",
        kid="old",
    )
    new = attrs.evolve(old, private_key=ec.generate_private_key(ec.SECP256R1()), kid="new")
    token = await _svc(old).issue_token(principal_id=uuid4())
    svc = AccessTokenService(signer=new, config=_svc().config, additional_verifiers=(old,))

    await svc.verify_token(token)
    await svc.verify_token(token)

    assert len(decodes) == 1


async def test_failures_are_not_cached(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc()
    forged = await _svc().issue_token(principal_id=uuid4())

    await _expect("invalid_access_token", svc, forged)
    await _expect("invalid_access_token", svc, forged)
    assert len(decodes) == 2


async def test_a_token_not_yet_valid_is_not_cached(clock: _Clock, decodes: list[str]) -> None:
    secret = secrets.token_bytes(32)
    svc = _svc(Hs256Signer(secret=secret))
    now = int(_T0.timestamp())
    token = jwt.encode(
        {
            "iss": "it",
            "aud": "api",
            "sub": str(uuid4()),
            "iat": now,
            "nbf": now + 60,
            "exp": now + 900,
        },
        secret,
        algorithm="HS256",
    )

    await _expect("invalid_access_token", svc, token)

    clock.advance(60)
    await svc.verify_token(token)
    await svc.verify_token(token)

    assert len(decodes) == 2, "the refused verification stored nothing; the accepted one did"


async def test_claims_are_copies(clock: _Clock) -> None:
    svc = _svc()
    token = await svc.issue_token(principal_id=uuid4())

    claims = await svc.verify_token(token)
    claims["sub"] = "someone else"

    assert (await svc.verify_token(token))["sub"] != "someone else"


async def test_nested_claims_are_not_cached(clock: _Clock, decodes: list[str]) -> None:
    secret = secrets.token_bytes(32)
    svc = _svc(Hs256Signer(secret=secret))
    now = int(_T0.timestamp())
    token = jwt.encode(
        {"iss": "it", "aud": ["api"], "sub": str(uuid4()), "iat": now, "exp": now + 900},
        secret,
        algorithm="HS256",
    )

    await svc.verify_token(token)
    await svc.verify_token(token)

    assert len(decodes) == 2


async def test_least_recently_used_goes_first(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc(verify_cache_max_entries=2)
    a, b, c = [await svc.issue_token(principal_id=uuid4()) for _ in range(3)]

    for token in (a, b, a, c, a, b):
        await svc.verify_token(token)

    # ``c`` evicted ``b`` (``a`` was used since); then ``b`` evicted ``c``.
    assert decodes == [a, b, c, b]


async def test_a_zero_ttl_disables_the_cache(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc(verify_cache_ttl=timedelta(0))
    token = await svc.issue_token(principal_id=uuid4())

    await svc.verify_token(token)
    await svc.verify_token(token)

    assert len(decodes) == 2


@pytest.mark.parametrize(
    "cfg",
    [
        {"verify_cache_ttl": timedelta(minutes=5, seconds=1)},
        {"verify_cache_ttl": timedelta(seconds=-1)},
        {"verify_cache_ttl": 30},
        {"verify_cache_max_entries": 0},
        {"verify_cache_max_entries": True},
    ],
)
def test_cache_settings_are_validated(cfg: dict[str, Any]) -> None:
    with pytest.raises(CoreException) as ei:
        AccessTokenConfig(**cfg)

    assert ei.value.kind == "configuration"


async def test_a_hit_still_checks_the_session(clock: _Clock, decodes: list[str]) -> None:
    svc = _svc()
    pid, sid = uuid4(), uuid4()
    token = await svc.issue_token(principal_id=pid, session_id=sid)

    session = MagicMock(principal_id=pid, tenant_id=None, revoked_at=None, rotated_at=None)
    session_qry = MagicMock()
    session_qry.spec.cache = None
    session_qry.spec.history_enabled = False
    session_qry.find = AsyncMock(return_value=session)
    verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)

    await verifier.verify_token(AccessTokenCredentials(token=token))
    session.revoked_at = _T0

    with pytest.raises(CoreException) as ei:
        await verifier.verify_token(AccessTokenCredentials(token=token))

    assert ei.value.code == "session_revoked"
    assert session_qry.find.await_count == 2
    assert len(decodes) == 1, "the claims came from the cache; the session check did not"
