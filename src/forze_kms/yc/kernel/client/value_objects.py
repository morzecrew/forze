from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcGeneratedDataKey:
    """A data key minted by Yandex Cloud KMS ``SymmetricCrypto.GenerateDataKey``.

    Unlike AWS (whose blob hides its key version) Yandex Cloud reports the key version
    that wrapped the key, so it is carried into the envelope for observability — the
    same way a Vault ``vault:vN:`` token surfaces its version. ``Decrypt`` selects the
    version from the ciphertext, so nothing *depends* on it: rotation stays transparent.
    """

    plaintext: bytes = attrs.field(repr=False)
    """The raw data key. ``repr`` suppressed — this is key material."""

    ciphertext: bytes
    """The wrapped data key, decryptable only by the KMS key."""

    version_id: str | None = None
    """The key version that wrapped it, when the backend reports one."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class YcKmsConfig:
    """Yandex Cloud KMS optional configuration."""

    endpoint: str | None = None
    """Override the Yandex Cloud API endpoint (``None`` = the SDK default)."""

    request_timeout: float | None = None
    """Per-call deadline in seconds (``None`` = the SDK default)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.request_timeout is not None and self.request_timeout <= 0:
            raise exc.configuration("Request timeout must be positive")
