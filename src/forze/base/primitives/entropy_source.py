"""Ambient, context-scoped source of randomness (bytes, bits, floats, random ids).

The entropy primitives used across the framework — AEAD nonces, backoff jitter,
opaque tokens, random ``uuid4`` ids — read the active :class:`EntropySource` rather
than ``secrets``/``random``/``os.urandom`` directly, so a scope can make every random
read deterministic and seed-replayable (simulation, deterministic tests) **without
changing call sites**.

This is the entropy twin of :mod:`forze.base.primitives.time_source`: same ContextVar
idiom, default = the real system CSPRNG so nothing changes unless a source is bound.

Note on security: the default :class:`SystemEntropySource` delegates to ``secrets`` and
``os.urandom`` and is byte-for-byte equivalent to the direct stdlib calls it replaces —
production keeps full cryptographic entropy. :class:`SeededEntropySource` is for
simulation only; it is **not** cryptographically secure and must never be bound in a
context that produces durable secrets.
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
    """A source of random bytes, bits, floats, and random (v4) ids."""

    def random_bytes(self, n: int) -> bytes:
        """Return *n* fresh random bytes (e.g. an AEAD nonce or opaque token)."""
        ...  # pragma: no cover

    def randbits(self, k: int) -> int:
        """Return a non-negative integer with *k* random bits."""
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


@final
@attrs.define(slots=True, frozen=True)
class SystemEntropySource:
    """The real system CSPRNG — the default source (identical to direct stdlib reads)."""

    def random_bytes(self, n: int) -> bytes:  # noqa: PYL-R0201
        return secrets.token_bytes(n)

    def randbits(self, k: int) -> int:  # noqa: PYL-R0201
        return secrets.randbits(k)

    def random(self) -> float:  # noqa: PYL-R0201
        return random.random()  # nosec B311 - system CSPRNG default

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

    Not cryptographically secure; intended only for deterministic simulation and tests.
    All four reads are driven by a single :class:`Random`, so the full sequence
    of bytes/bits/floats/ids is a deterministic function of ``seed``.
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

    def random_bytes(self, n: int) -> bytes:
        return self._rng.randbytes(n)

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
    """Return the entropy source active in the current context."""

    return _ENTROPY_SOURCE.get()


# ....................... #


@contextmanager
def bind_entropy_source(source: EntropySource) -> Iterator[None]:
    """Bind *source* as the active entropy source for the duration of the block."""

    token = _ENTROPY_SOURCE.set(source)

    try:
        yield

    finally:
        _ENTROPY_SOURCE.reset(token)


# ....................... #


def token_urlsafe(nbytes: int) -> str:
    """Seam-routed equivalent of :func:`secrets.token_urlsafe`.

    Returns a URL-safe base64 text token (padding stripped) drawn from the active
    entropy source — so opaque tokens become deterministic under a bound source.
    """

    raw = current_entropy_source().random_bytes(nbytes)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


# ....................... #


def derive_seed(seed: int, label: str) -> int:
    """Derive a stable, independent sub-seed from a master *seed*, keyed by *label*.

    Cross-machine / cross-process stable — a fixed hash, **not** Python's ``hash()`` (which
    is PYTHONHASHSEED-salted) — and **order-insensitive**: keyed by *label*, so adding a new
    derived stream never shifts existing sub-seeds, and a saved (regression) seed keeps its
    meaning. Lets one master seed drive several independent nondeterminism streams
    (schedule, faults, entropy, inputs) that vary independently yet reproduce exactly.
    """

    digest = hashlib.blake2b(f"{seed}:{label}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")
