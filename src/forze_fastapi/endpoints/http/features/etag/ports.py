from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class ETagProviderPort(Protocol):
    """Port for deriving an ETag value from a serialized response.

    Implementations receive the raw response body bytes and must return
    a stable, opaque tag string (without surrounding quotes) or ``None``
    when the response is not eligible for ETag generation.
    """

    def __call__(self, response_body: bytes) -> str | None:
        """Derive a stable tag string from *response_body*.

        :param response_body: Serialized response payload.
        :returns: Raw ETag value or ``None`` to skip ETag injection.
        """
        ...  # pragma: no cover
