"""Two ambient, context-scoped sources of randomness — split by security posture at the type level.

Randomness in the framework serves two irreconcilable purposes, so it is two distinct seams:

- :class:`EntropySource` — **replayable** randomness: backoff jitter, load-shed sampling, random
  (v4/v7) ids. A scope can bind a seeded source (:class:`SeededEntropySource`) to make every such
  read a deterministic function of a seed (simulation, deterministic tests) *without changing call
  sites*. Replayable means predictable, so this seam draws **no durable secrets** — its surface has no
  byte-minting method at all.
- :class:`SecretEntropy` — **durable-secret** randomness: AEAD/GCM nonces, opaque tokens, API keys,
  data-encryption keys. Served only by :func:`secure_random_bytes` / :func:`secure_token_urlsafe`,
  which read this seam, never the replayable one. The only implementation is the CSPRNG
  :class:`SystemSecretEntropy`; there is **no seeded ``SecretEntropy``**, so a seeded/replayable
  source *physically cannot* mint a nonce, token, or key — it is a different type that lacks
  ``secret_bytes``. This is a type-level guarantee, not a runtime flag: a simulation binds the seeded
  *replayable* seam for reproducibility while durable secrets keep drawing full CSPRNG entropy, and no
  ``permit`` escape hatch exists to weaken that. (Secrets are outside the byte-identical-replay
  envelope by design — value traces redact them — so this costs no reproducibility that mattered.)

Both seams mirror :mod:`forze.base.primitives.time_source`: the same ``ContextVar`` idiom, default =
the real system CSPRNG so nothing changes unless a source is bound.
"""

import base64
import hashlib
import random
import secrets
from contextlib import contextmanager
from contextvars import ContextVar
from random import Random
from typing import Iterator, Protocol, final, runtime_checkable
from uuid import UUID
from uuid import uuid4 as uuid4_func

import attrs

# ----------------------- #


@runtime_checkable
class EntropySource(Protocol):
    """A source of **replayable** randomness — bits, floats, and random (v4) ids.

    Deliberately has no ``bytes``-minting method: raw random bytes feed durable secrets, which must
    never come from a seedable (predictable) source. Draw those from :class:`SecretEntropy` via
    :func:`secure_random_bytes` instead.
    """

    def randbits(self, k: int) -> int:
        """Return a non-negative integer with *k* random bits (e.g. a uuid7's random component)."""
        ...  # pragma: no cover

    def random(self) -> float:
        """Return a random float in the half-open interval ``[0.0, 1.0)``."""
        ...  # pragma: no cover

    def uuid4(self) -> UUID:
        """Return a fresh random (version 4) UUID."""
        ...  # pragma: no cover

    def as_random(self) -> Random:
        """Return a ``Random``-compatible generator drawn from this source.

        Bridges call sites that need the broader stdlib ``random`` API
        (``uniform``, ``randrange``, …) — e.g. backoff/jitter — to the seam.
        """
        ...  # pragma: no cover


# ....................... #


_SYSTEM_RANDOM = random.SystemRandom()
"""Process-wide os.urandom-backed generator (stateless, thread-safe) so every
:class:`SystemEntropySource` read — floats included — is CSPRNG-backed, not the
process-global Mersenne Twister."""


@final
@attrs.define(slots=True, frozen=True)
class SystemEntropySource:
    """The real system CSPRNG — the default replayable source (identical to direct stdlib reads)."""

    def randbits(self, k: int) -> int:  # noqa: PYL-R0201
        return secrets.randbits(k)

    def random(self) -> float:  # noqa: PYL-R0201
        # os.urandom-backed, not the process-global Mersenne Twister (``random.random``),
        # so this source is CSPRNG-backed across *all* reads as its name/docstring claim.
        return _SYSTEM_RANDOM.random()

    def uuid4(self) -> UUID:  # noqa: PYL-R0201
        return uuid4_func()

    def as_random(self) -> Random:  # noqa: PYL-R0201
        # A fresh CSPRNG-backed generator (os.urandom under the hood), matching the
        # non-deterministic intent of the jitter/backoff call sites it serves.
        return random.SystemRandom()


# ....................... #


@final
@attrs.define(slots=True)
class SeededEntropySource:
    """A seeded PRNG for simulation: same seed → identical, replayable random stream.

    Not cryptographically secure, and — by construction — not a :class:`SecretEntropy`: it has no
    ``secret_bytes``, so it can drive jitter/sampling/ids reproducibly but can never mint a durable
    secret. All reads are driven by a single :class:`Random`, so the full sequence of bits/floats/ids
    is a deterministic function of ``seed``.
    """

    seed: int
    _rng: Random = attrs.field(
        default=attrs.Factory(
            lambda self: Random(
                self.seed
            ),  # nosec B311 - deterministic sim RNG, not crypto
            takes_self=True,
        ),
        init=False,
    )

    # ....................... #

    def randbits(self, k: int) -> int:
        return self._rng.getrandbits(k)

    def random(self) -> float:
        return self._rng.random()

    def uuid4(self) -> UUID:
        # ``version=4`` overwrites the version/variant bits of the 128 random bits,
        # matching stdlib ``uuid4`` layout while keeping the draw fully seeded.
        return UUID(int=self._rng.getrandbits(128), version=4)

    def as_random(self) -> Random:
        # The same persistent seeded generator backing the other reads, so the full
        # stream — including stdlib-API draws — stays a deterministic function of seed.
        return self._rng


# ....................... #

_ENTROPY_SOURCE: ContextVar[EntropySource] = ContextVar(
    "entropy_source",
    default=SystemEntropySource(),
)

# ....................... #


def current_entropy_source() -> EntropySource:
    """Return the replayable entropy source active in the current context."""

    return _ENTROPY_SOURCE.get()


# ....................... #


@contextmanager
def bind_entropy_source(source: EntropySource) -> Iterator[None]:
    """Bind *source* as the active replayable entropy source for the duration of the block."""

    token = _ENTROPY_SOURCE.set(source)

    try:
        yield

    finally:
        _ENTROPY_SOURCE.reset(token)


# ----------------------- #
# Durable-secret entropy — a separate seam a seeded source cannot satisfy.


@runtime_checkable
class SecretEntropy(Protocol):
    """A source of random bytes for **durable secrets** (AEAD nonces, tokens, keys).

    A distinct type from :class:`EntropySource` with a disjoint surface: a replayable/seeded source
    is not a ``SecretEntropy`` (it has no ``secret_bytes``), so it cannot be bound here or passed to
    :func:`secure_random_bytes`. The only shipped implementation is the CSPRNG
    :class:`SystemSecretEntropy`; there is no seeded one, so a durable secret is never predictable.
    """

    def secret_bytes(self, n: int) -> bytes:
        """Return *n* cryptographically-secure random bytes for a durable secret."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class SystemSecretEntropy:
    """The system CSPRNG (``secrets``/``os.urandom``) — the only source of durable-secret bytes."""

    def secret_bytes(self, n: int) -> bytes:  # noqa: PYL-R0201
        return secrets.token_bytes(n)


# ....................... #

_SECRET_ENTROPY: ContextVar[SecretEntropy] = ContextVar(
    "secret_entropy",
    default=SystemSecretEntropy(),
)
"""The active durable-secret source. Defaults to the CSPRNG and is **not** bound by the simulation
runtime — so nonces/tokens/keys keep full entropy even under a seeded, replayable simulation."""


# ....................... #


def current_secret_entropy() -> SecretEntropy:
    """Return the durable-secret entropy source active in the current context (the CSPRNG default)."""

    return _SECRET_ENTROPY.get()


# ....................... #


@contextmanager
def bind_secret_entropy(source: SecretEntropy) -> Iterator[None]:
    """Bind *source* as the active durable-secret source for the block.

    Accepts only a :class:`SecretEntropy` — a seeded/replayable :class:`EntropySource` is a different
    type and is rejected at type-check, so this cannot be used to make secrets predictable. Intended
    for a test that substitutes its own ``SecretEntropy`` (a known-answer fixture, a fault injector);
    the simulation runtime never calls it.
    """

    token = _SECRET_ENTROPY.set(source)

    try:
        yield

    finally:
        _SECRET_ENTROPY.reset(token)


# ....................... #


def secure_random_bytes(n: int) -> bytes:
    """Return *n* CSPRNG bytes for a durable secret (AEAD nonce, token, API key, data key).

    Reads the :class:`SecretEntropy` seam — never the replayable :class:`EntropySource` — so the draw
    is cryptographically secure by construction, even inside a seeded simulation. There is no seeded
    ``SecretEntropy`` and no runtime opt-out: a predictable secret is unrepresentable, not merely
    discouraged.
    """

    return current_secret_entropy().secret_bytes(n)


# ....................... #


def secure_token_urlsafe(nbytes: int) -> str:
    """A URL-safe base64 text token (padding stripped) drawn from :func:`secure_random_bytes`.

    For security-sensitive tokens (OAuth state, PKCE verifier) — CSPRNG by construction.
    """

    raw = secure_random_bytes(nbytes)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


# ....................... #


def derive_seed(seed: int, label: str) -> int:
    """Derive a stable, independent sub-seed from a master *seed*, keyed by *label*.

    Cross-machine / cross-process stable — a fixed hash, **not** Python's ``hash()`` (which is
    PYTHONHASHSEED-salted) — and **order-insensitive**: keyed by *label*, so adding a new derived
    stream never shifts existing sub-seeds, and a saved (regression) seed keeps its meaning. Lets one
    master seed drive several independent nondeterminism streams (schedule, faults, entropy, inputs)
    that vary independently yet reproduce exactly.
    """

    digest = hashlib.blake2b(f"{seed}:{label}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")
