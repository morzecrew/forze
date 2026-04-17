from forze_simple_auth._compat import require_simple_auth

require_simple_auth()

# ....................... #


import attrs
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerificationError, VerifyMismatchError

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordHasherConfig:
    """Password hasher configuration."""

    time_cost: int = 2
    memory_cost: int = 102_400  # 100 MiB
    parallelism: int = 8
    hash_length: int = 32
    salt_length: int = 16


# ....................... #


@attrs.define(slots=True, kw_only=True)
class PasswordHasherService:
    """Password hasher."""

    config: PasswordHasherConfig = attrs.field(factory=PasswordHasherConfig)

    # Non initable fields
    __hasher: PasswordHasher | None = attrs.field(default=None, init=False)

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
