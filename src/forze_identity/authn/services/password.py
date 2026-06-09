import attrs
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerificationError, VerifyMismatchError

# Sentinel for constant-time verification when no account exists (not user-facing).
_TIMING_DUMMY_PASSWORD = "__forze_timing_mitigation__"  # nosec B105 — non-user sentinel for timing-safe verify

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordConfig:
    """Password hasher configuration."""

    time_cost: int = 2
    memory_cost: int = 102_400  # 100 MiB
    parallelism: int = 8
    hash_length: int = 32
    salt_length: int = 16


# ....................... #


@attrs.define(slots=True, kw_only=True)
class PasswordService:
    """Password hasher service."""

    config: PasswordConfig = attrs.field(factory=PasswordConfig)

    # Non initable fields
    __hasher: PasswordHasher | None = attrs.field(default=None, init=False)
    __timing_dummy_hash: str | None = attrs.field(default=None, init=False)

    # ....................... #

    def _require_hasher(self) -> PasswordHasher:
        if self.__hasher is not None:
            return self.__hasher

        self.__hasher = PasswordHasher(
            time_cost=self.config.time_cost,
            memory_cost=self.config.memory_cost,
            parallelism=self.config.parallelism,
            hash_len=self.config.hash_length,
            salt_len=self.config.salt_length,
        )

        return self.__hasher

    # ....................... #

    def hash_password(self, password: str) -> str:
        ph = self._require_hasher()

        return ph.hash(password)

    # ....................... #

    def timing_dummy_hash(self) -> str:
        """Argon2 hash used for verify work when no matching account exists."""

        if self.__timing_dummy_hash is None:
            self.__timing_dummy_hash = self.hash_password(_TIMING_DUMMY_PASSWORD)

        return self.__timing_dummy_hash

    # ....................... #

    def verify_password(self, password_hash: str, password: str) -> bool:
        try:
            ph = self._require_hasher()

            return ph.verify(password_hash, password)

        except (InvalidHash, VerificationError, VerifyMismatchError):
            return False

    # ....................... #

    def password_needs_rehash(self, password_hash: str) -> bool:
        try:
            ph = self._require_hasher()

            return ph.check_needs_rehash(password_hash)

        except InvalidHash:
            return True
