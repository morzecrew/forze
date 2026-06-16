import asyncio
from concurrent.futures import ThreadPoolExecutor

import attrs
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHash, VerificationError, VerifyMismatchError

# Sentinel for constant-time verification when no account exists (not user-facing).
_TIMING_DUMMY_PASSWORD = "__forze_timing_mitigation__"  # nosec B105 # skipcq: SCT-A000 — non-user sentinel for timing-safe verify

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordConfig:
    """Password hasher configuration."""

    time_cost: int = 2
    memory_cost: int = 102_400  # 100 MiB
    parallelism: int = 8
    hash_length: int = 32
    salt_length: int = 16

    hashing_concurrency: int = attrs.field(default=4, validator=attrs.validators.ge(1))
    """Maximum Argon2 operations running at once across the service.

    Each in-flight hash/verify holds ``memory_cost`` KiB of working memory, so
    peak hashing memory is bounded by ``hashing_concurrency * memory_cost``
    (~400 MiB with the defaults). Excess logins queue instead of allocating —
    this is what keeps an unauthenticated login flood from becoming a memory
    blowup. Raise it to trade memory for login throughput.
    """


# ....................... #


@attrs.define(slots=True, kw_only=True)
class PasswordService:
    """Password hasher service.

    Argon2 is deliberately expensive (tens of milliseconds of CPU plus
    ``memory_cost`` of working memory per call), so the async methods run it on
    a dedicated bounded thread pool: inline execution would stall the event
    loop for every request, and offloading to the shared default executor would
    let a burst of login attempts allocate unbounded hashing memory. argon2-cffi
    releases the GIL during hashing, so offloaded calls run genuinely in
    parallel up to :attr:`PasswordConfig.hashing_concurrency`.

    Request paths use the async methods; the ``*_sync`` variants block the
    calling thread and exist for seeding scripts, CLIs, and tests.
    """

    config: PasswordConfig = attrs.field(factory=PasswordConfig)

    # Non initable fields
    __hasher: PasswordHasher | None = attrs.field(default=None, init=False)
    __executor: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
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

    def _require_executor(self) -> ThreadPoolExecutor:
        # Lazy and unguarded: callers sit on one event loop, and there is no
        # await between the check and the assignment.
        if self.__executor is None:
            self.__executor = ThreadPoolExecutor(
                max_workers=self.config.hashing_concurrency,
                thread_name_prefix="forze-argon2",
            )

        return self.__executor

    # ....................... #

    def hash_password_sync(self, password: str) -> str:
        """Blocking Argon2 hash; for seeding scripts, CLIs, and tests."""

        ph = self._require_hasher()

        return ph.hash(password)

    # ....................... #

    async def hash_password(self, password: str) -> str:
        loop = asyncio.get_running_loop()

        return await loop.run_in_executor(
            self._require_executor(),
            self.hash_password_sync,
            password,
        )

    # ....................... #

    async def timing_dummy_hash(self) -> str:
        """Argon2 hash used for verify work when no matching account exists."""

        if self.__timing_dummy_hash is None:
            # Concurrent first calls may compute this twice; both results are
            # valid dummies and the cached value is stable afterwards.
            self.__timing_dummy_hash = await self.hash_password(
                _TIMING_DUMMY_PASSWORD,
            )

        return self.__timing_dummy_hash

    # ....................... #

    def verify_password_sync(self, password_hash: str, password: str) -> bool:
        """Blocking Argon2 verify; for seeding scripts, CLIs, and tests."""

        try:
            ph = self._require_hasher()

            return ph.verify(password_hash, password)

        except (InvalidHash, VerificationError, VerifyMismatchError):
            return False

    # ....................... #

    async def verify_password(self, password_hash: str, password: str) -> bool:
        loop = asyncio.get_running_loop()

        return await loop.run_in_executor(
            self._require_executor(),
            self.verify_password_sync,
            password_hash,
            password,
        )

    # ....................... #

    def password_needs_rehash(self, password_hash: str) -> bool:
        """Whether ``password_hash`` was produced with outdated Argon2 parameters.

        Stays synchronous: this only parses the hash's parameter header, it
        performs no hashing.
        """

        try:
            ph = self._require_hasher()

            return ph.check_needs_rehash(password_hash)

        except InvalidHash:
            return True
