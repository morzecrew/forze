"""Unit tests for cache contract (CacheSpec, CacheDepKey)."""

from datetime import timedelta


from forze.application.contracts.cache import CacheDepKey, CacheSpec

# ----------------------- #


class TestCacheSpec:
    """Tests for CacheSpec."""

    def test_name_required(self) -> None:
        spec = CacheSpec(name="test")
        assert spec.name == "test"

    def test_default_ttl(self) -> None:
        spec = CacheSpec(name="ns")
        assert spec.ttl == timedelta(seconds=300)

    def test_custom_ttl(self) -> None:
        spec = CacheSpec(name="ns", ttl=timedelta(seconds=60))
        assert spec.ttl == timedelta(seconds=60)


class TestCacheDepKey:
    """Tests for CacheDepKey."""

    def test_cache_dep_key_name(self) -> None:
        assert CacheDepKey.name == "cache"


class TestExecutionContextCache:
    """Tests for ExecutionContext.cache() resolution."""

    def test_cache_resolves_registered_port(
        self,
        stub_ctx,
    ) -> None:
        """ctx.cache(spec) returns CachePort from CacheDepKey."""
        from forze.application.contracts.cache import CacheSpec

        spec = CacheSpec(name="test")
        port = stub_ctx.cache(spec)
        assert port is not None
        assert hasattr(port, "get") and hasattr(port, "set")
