"""Self-hosted key management for Forze envelope encryption — no cloud, no extra.

Wraps data keys under operator-provided raw 32-byte master keys with
AES-256-GCM, entirely in process. The only dependency is ``cryptography`` via
:class:`~forze.base.crypto.AesGcmAead`, which is already a core dependency, so
this subpackage — unlike its cloud siblings — needs no extra to import.

See :class:`~forze_kms.local.LocalKeyManagement` for the threat model (master
keys live in process memory and configuration; the operator's host is the trust
boundary) and for wiring and rotation-overlap examples.
"""

from .adapters import LocalKeyManagement

# ----------------------- #

__all__ = [
    "LocalKeyManagement",
]
