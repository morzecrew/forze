"""Dependency-light crypto primitives: the envelope wire format and AEAD seam.

This package owns only pure, stdlib-backed building blocks — the
self-describing :class:`EncryptedEnvelope` and the :class:`Aead` protocol. Key
management (the async, DI-injected service) and the orchestrating cipher live in
:mod:`forze.application.contracts.crypto`; concrete ciphers and key backends
live in integration packages.
"""

from .aead import Aead
from .chunked import (
    DEFAULT_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    ChunkedHeader,
    ChunkedStreamReader,
    ChunkFrame,
    chunk_frame_stride,
    is_chunked_envelope,
    open_chunk,
    pack_chunked_header,
    parse_frame,
    seal_chunk,
    unpack_chunked_header,
)
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
    "DEFAULT_CHUNK_SIZE",
    "MAX_CHUNK_SIZE",
    "Aead",
    "AesGcmAead",
    "ChaCha20Poly1305Aead",
    "ChunkFrame",
    "ChunkedHeader",
    "ChunkedStreamReader",
    "EncryptedEnvelope",
    "chunk_frame_stride",
    "ensure_algorithm",
    "is_chunked_envelope",
    "is_envelope",
    "open_chunk",
    "pack_chunked_header",
    "pack_envelope",
    "parse_frame",
    "seal_chunk",
    "unpack_chunked_header",
    "unpack_envelope",
]
