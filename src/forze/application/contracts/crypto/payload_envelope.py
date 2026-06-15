"""Wire marker for a whole-payload encrypted message.

A whole-payload envelope (outbox events, end-to-end queue messages) travels as a
one-key JSON wrapper ``{"<sentinel>": "<base64 envelope>"}`` so plaintext and ciphertext
are trivially distinguishable on the wire. This module owns just the marker and cheap
detection — the encrypt/decrypt logic (keyring + AAD) lives in the integration layer. It
sits in ``contracts.crypto`` so both the outbox and the transport codecs can share it
without one integration importing another.
"""

from forze.base.primitives import JsonDict

# ----------------------- #

ENCRYPTED_PAYLOAD_KEY = "__fz_enc__"
"""Single key marking a whole-payload encrypted message wrapper."""

_BODY_PREFIX = b'{"' + ENCRYPTED_PAYLOAD_KEY.encode("ascii") + b'":'
"""Serialized prefix of a wrapper (orjson, single key, no spaces) — a cheap body peek."""

# ....................... #


def wrap_encrypted_payload(ciphertext_b64: str) -> JsonDict:
    """Wrap base64 ciphertext as the one-key envelope payload."""

    return {ENCRYPTED_PAYLOAD_KEY: ciphertext_b64}


# ....................... #


def is_encrypted_payload(payload: object) -> bool:
    """Return whether *payload* is a whole-payload encrypted wrapper (vs plaintext)."""

    return (
        isinstance(payload, dict)
        and len(payload) == 1  # pyright: ignore[reportUnknownArgumentType]
        and ENCRYPTED_PAYLOAD_KEY in payload
    )


# ....................... #


def encrypted_payload_ciphertext(payload: JsonDict) -> str:
    """Return the base64 ciphertext from a wrapper (caller ensures it is one)."""

    return payload[ENCRYPTED_PAYLOAD_KEY]


# ....................... #


def looks_encrypted_body(body: bytes) -> bool:
    """Cheaply peek whether a serialized message *body* is an encrypted wrapper.

    Lets a transport codec divert a wrapper to the decrypt path without fully parsing
    every message — the plaintext decode path stays byte-for-byte unchanged.
    """

    return body[: len(_BODY_PREFIX)] == _BODY_PREFIX
