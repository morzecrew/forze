"""Dependency-light crypto primitives: the envelope wire format and AEAD seam.

This package owns only pure, stdlib-backed building blocks — the
self-describing :class:`EncryptedEnvelope` and the :class:`Aead` protocol. Key
management (the async, DI-injected service) and the orchestrating cipher live in
:mod:`forze.application.contracts.crypto`; concrete ciphers and key backends
live in integration packages.
"""

from .aead import Aead
from .ciphers import AesGcmAead, ChaCha20Poly1305Aead
from .envelope import (
    ENVELOPE_B64_PREFIX,
    EncryptedEnvelope,
    ensure_algorithm,
    is_envelope,
    pack_envelope,
    unpack_envelope,
)

# ----------------------- #

__all__ = [
    "ENVELOPE_B64_PREFIX",
    "Aead",
    "AesGcmAead",
    "ChaCha20Poly1305Aead",
    "EncryptedEnvelope",
    "ensure_algorithm",
    "is_envelope",
    "pack_envelope",
    "unpack_envelope",
]
