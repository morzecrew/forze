"""Runtime checks for ETag provider protocol."""

from forze_fastapi.endpoints.http.features.etag.ports import ETagProviderPort


class _StubEtag:
    def __call__(self, response_body: bytes) -> str | None:
        if not response_body:
            return None
        return str(len(response_body))


def test_etag_provider_port_runtime_checkable() -> None:
    stub = _StubEtag()
    assert isinstance(stub, ETagProviderPort)
    assert stub(b"abc") == "3"
    assert stub(b"") is None


def test_non_conforming_not_instance() -> None:
    class Bad:
        pass

    assert not isinstance(Bad(), ETagProviderPort)
