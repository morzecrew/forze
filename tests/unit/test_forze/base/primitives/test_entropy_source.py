"""Tests for the two entropy seams: replayable randomness and CSPRNG durable-secret entropy."""

from __future__ import annotations

from datetime import UTC, datetime

from forze.base.primitives import (
    FrozenTimeSource,
    SecretEntropy,
    SeededEntropySource,
    SystemEntropySource,
    SystemSecretEntropy,
    bind_entropy_source,
    bind_secret_entropy,
    bind_time_source,
    current_entropy_source,
    current_secret_entropy,
    derive_seed,
    secure_random_bytes,
    secure_token_urlsafe,
    uuid4,
    uuid7,
)

# ----------------------- #

_T0 = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)


class TestDefaultBehavior:
    def test_default_source_is_system(self) -> None:
        assert isinstance(current_entropy_source(), SystemEntropySource)

    def test_randbits_in_range(self) -> None:
        for _ in range(50):
            assert 0 <= SystemEntropySource().randbits(8) < 2**8

    def test_random_in_unit_interval(self) -> None:
        value = SystemEntropySource().random()
        assert 0.0 <= value < 1.0

    def test_random_is_csprng_not_global_mersenne_twister(self) -> None:
        """``random()`` must draw from os.urandom (as the source claims), not the
        process-global Mersenne Twister — so seeding ``random`` can't make it reproduce."""

        import random as _stdlib_random

        _stdlib_random.seed(12345)
        first = SystemEntropySource().random()
        _stdlib_random.seed(12345)
        second = SystemEntropySource().random()

        # With the global MT these would be identical; from os.urandom they (almost
        # surely, ~2**-53 collision) differ.
        assert first != second
        assert 0.0 <= first < 1.0

    def test_uuid4_is_version_4(self) -> None:
        assert SystemEntropySource().uuid4().version == 4

    def test_system_uuid4s_are_distinct(self) -> None:
        src = SystemEntropySource()
        assert src.uuid4() != src.uuid4()


class TestSeededReproducibility:
    def test_same_seed_same_stream(self) -> None:
        a, b = SeededEntropySource(seed=42), SeededEntropySource(seed=42)
        assert a.randbits(32) == b.randbits(32)
        assert a.random() == b.random()
        assert a.uuid4() == b.uuid4()

    def test_different_seeds_diverge(self) -> None:
        a, b = SeededEntropySource(seed=1), SeededEntropySource(seed=2)
        assert a.uuid4() != b.uuid4()

    def test_seeded_uuid4_is_version_4(self) -> None:
        assert SeededEntropySource(seed=7).uuid4().version == 4

    def test_stream_advances_within_one_source(self) -> None:
        src = SeededEntropySource(seed=99)
        assert src.uuid4() != src.uuid4()


class TestBoundSource:
    def test_bind_controls_uuid4(self) -> None:
        with bind_entropy_source(SeededEntropySource(seed=5)):
            first = uuid4()
        with bind_entropy_source(SeededEntropySource(seed=5)):
            second = uuid4()
        assert first == second  # same seed → same random id

    def test_bind_restores_previous_source_on_exit(self) -> None:
        with bind_entropy_source(SeededEntropySource(seed=5)):
            assert isinstance(current_entropy_source(), SeededEntropySource)
        assert isinstance(current_entropy_source(), SystemEntropySource)

    def test_nested_binds(self) -> None:
        with bind_entropy_source(SeededEntropySource(seed=1)):
            outer = current_entropy_source()
            with bind_entropy_source(SeededEntropySource(seed=2)):
                assert current_entropy_source() is not outer
            assert current_entropy_source() is outer

    def test_derived_uuid4_unaffected_by_seam(self) -> None:
        # The value-derived (hashed) uuid4 path never draws entropy.
        with bind_entropy_source(SeededEntropySource(seed=5)):
            assert uuid4("stable-key") == uuid4("stable-key")


class TestExplicitTimestampUuid7IsSeamed:
    """Wrinkle #1: uuid7()'s explicit-timestamp branch draws its low bits from the
    entropy seam, so a bound source makes the *full* uuid (not just the prefix)
    deterministic — and FrozenTimeSource becomes fully replayable."""

    def test_explicit_timestamp_deterministic_under_seed(self) -> None:
        with bind_entropy_source(SeededEntropySource(seed=3)):
            a = uuid7(timestamp_ms=1_700_000_000_000)
        with bind_entropy_source(SeededEntropySource(seed=3)):
            b = uuid7(timestamp_ms=1_700_000_000_000)
        assert a == b  # identical random bits, not just identical timestamp

    def test_frozen_time_plus_seed_is_byte_identical(self) -> None:
        def _draw() -> tuple[object, ...]:
            with bind_time_source(FrozenTimeSource(instant=_T0)):
                with bind_entropy_source(SeededEntropySource(seed=11)):
                    return (uuid7(), uuid7(), uuid4(), uuid4())

        assert _draw() == _draw()

    def test_explicit_timestamp_still_random_by_default(self) -> None:
        # Without a bound source, the system CSPRNG keeps ids unpredictable.
        a = uuid7(timestamp_ms=1_700_000_000_000)
        b = uuid7(timestamp_ms=1_700_000_000_000)
        assert a != b


class TestDeriveSeed:
    """One master seed → independent, stable sub-seeds per stream."""

    def test_deterministic(self) -> None:
        assert derive_seed(0, "schedule") == derive_seed(0, "schedule")

    def test_stable_value_across_runs(self) -> None:
        # A fixed hash (not PYTHONHASHSEED-salted ``hash()``) → a constant across processes.
        assert derive_seed(0, "schedule") == 4223464447449377271

    def test_independent_by_label(self) -> None:
        # Different streams of the same seed must not coincide.
        labels = ("schedule", "fault", "entropy", "input")
        derived = {derive_seed(7, label) for label in labels}
        assert len(derived) == len(labels)

    def test_independent_by_master(self) -> None:
        assert derive_seed(0, "schedule") != derive_seed(1, "schedule")

    def test_order_insensitive(self) -> None:
        # Keyed by label, not position: adding a new stream never shifts existing sub-seeds.
        assert derive_seed(5, "fault") == derive_seed(5, "fault")
        # "schedule" is unaffected by whether other labels are derived before/after it.
        before = derive_seed(5, "schedule")
        derive_seed(5, "a_new_stream_added_later")
        assert derive_seed(5, "schedule") == before


# ....................... #


class _FixedSecret:
    """A test-only SecretEntropy that returns constant bytes (a known-answer fixture)."""

    def secret_bytes(self, n: int) -> bytes:
        return b"\x00" * n


class TestSecretEntropySeam:
    """Durable secrets read a separate CSPRNG seam that a seeded/replayable source cannot satisfy —
    enforced by the type, not a runtime flag."""

    def test_default_secret_source_is_the_csprng(self) -> None:
        assert isinstance(current_secret_entropy(), SystemSecretEntropy)

    def test_secure_helpers_produce_bytes_and_tokens(self) -> None:
        assert len(secure_random_bytes(12)) == 12
        assert isinstance(secure_token_urlsafe(16), str)

    def test_secure_draw_stays_csprng_under_a_seeded_replayable_bind(self) -> None:
        # THE guarantee: binding the seeded (simulation) *replayable* source does not make secrets
        # predictable. secure_random_bytes reads the SecretEntropy seam, which stays CSPRNG, so two
        # draws under the same seed differ — a seeded source cannot serve a nonce/token/key.
        with bind_entropy_source(SeededEntropySource(seed=1)):
            first = secure_random_bytes(16)
        with bind_entropy_source(SeededEntropySource(seed=1)):
            second = secure_random_bytes(16)
        assert first != second

    def test_a_seeded_source_is_not_a_secret_entropy(self) -> None:
        # Disjoint surfaces: the replayable seeded source has no secret_bytes, so it is not a
        # SecretEntropy (and the type checker rejects binding it as one).
        assert isinstance(SystemSecretEntropy(), SecretEntropy)
        assert not isinstance(SeededEntropySource(seed=1), SecretEntropy)
        assert not hasattr(SeededEntropySource(seed=1), "secret_bytes")

    def test_the_replayable_seam_has_no_byte_minting_method(self) -> None:
        # There is no way to route a durable secret through the replayable seam: it mints no bytes.
        assert not hasattr(current_entropy_source(), "random_bytes")

    def test_bind_secret_entropy_substitutes_the_secret_source(self) -> None:
        # A test may bind its own SecretEntropy (a fixed known-answer source); the type — not a
        # runtime flag — is what admits it, and the default is restored on exit.
        with bind_secret_entropy(_FixedSecret()):
            assert secure_random_bytes(4) == b"\x00\x00\x00\x00"
        assert isinstance(current_secret_entropy(), SystemSecretEntropy)
