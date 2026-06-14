"""HashiCorp Vault KV v2 client backed by ``hvac``."""

from forze_vault._compat import require_vault

require_vault()

# ....................... #

import asyncio
import base64
import contextlib
from typing import Any, cast, final

import attrs
import hvac
import requests
from hvac.exceptions import InvalidPath, VaultError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from forze.base.exceptions import exc
from forze.base.primitives import GuardedLifecycle, JsonDict
from forze_vault.kernel._logger import logger

from .port import VaultClientPort
from .value_objects import VaultConfig

# ----------------------- #

RENEW_FAILURE_RETRY_SECONDS = 30.0
"""Backoff before retrying a failed token renewal."""

MIN_RENEW_DELAY_SECONDS = 1.0
"""Lower bound for the derived renewal cadence."""

# ----------------------- #


@final
@attrs.define(slots=True)
class VaultClient(VaultClientPort):
    """Async-friendly Vault KV v2 reader using a blocking ``hvac`` client in a thread pool."""

    config: VaultConfig
    _client: Any = attrs.field(default=None, init=False, repr=False)
    _lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)
    _renew_task: asyncio.Task[None] | None = attrs.field(
        default=None, init=False, repr=False
    )

    # ....................... #

    async def initialize(self) -> None:
        async def setup() -> None:
            self._client = await asyncio.to_thread(self._create_client)

            if self.config.renew_token:
                await self._start_token_renewal()

        await self._lifecycle.initialize(
            setup,
            ready=lambda: self._client is not None,
        )

    # ....................... #

    async def close(self) -> None:
        async def teardown() -> None:
            # Stop the token-renewal background task before dropping the
            # client so renewal never races a torn-down client.
            task, self._renew_task = self._renew_task, None

            if task is not None:
                task.cancel()

                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(task)

            self._client = None

        await self._lifecycle.close(teardown)

    # ....................... #
    # Token renewal

    async def _start_token_renewal(self) -> None:
        lookup = await asyncio.to_thread(self._client.auth.token.lookup_self)
        data = cast(JsonDict, lookup.get("data") or {})

        if not data.get("renewable", False):
            logger.warning(
                "Vault token is not renewable, skipping background renewal",
            )
            return

        ttl = float(data.get("ttl") or 0)
        self._renew_task = asyncio.create_task(
            self._renew_loop(ttl),
            name="vault-token-renewal",
        )

    # ....................... #

    def _renew_delay(self, ttl_seconds: float) -> float:
        if self.config.renew_interval is not None:
            return self.config.renew_interval.total_seconds()

        return max(ttl_seconds / 2.0, MIN_RENEW_DELAY_SECONDS)

    # ....................... #

    async def _renew_loop(self, ttl_seconds: float) -> None:
        """Renew the token lease on a cadence; never crashes the app.

        Renewal extends a renewable token's lease — it cannot resurrect an
        expired or ``max_ttl``-capped token; those require a restart or an
        externally re-issued token.
        """

        delay = self._renew_delay(ttl_seconds)

        while True:
            await asyncio.sleep(delay)

            client = self._client

            if client is None:  # pragma: no cover - close() cancels first
                return

            try:
                response = await asyncio.to_thread(client.auth.token.renew_self)

            except asyncio.CancelledError:
                raise

            except Exception as e:  # noqa: BLE001 - renewal must not crash
                logger.warning(
                    "Vault token renewal failed, retrying in "
                    f"{RENEW_FAILURE_RETRY_SECONDS:.0f}s: {e}",
                )
                delay = RENEW_FAILURE_RETRY_SECONDS
                continue

            auth = cast(JsonDict, response.get("auth") or {})
            new_ttl = float(auth.get("lease_duration") or 0)

            if new_ttl > 0:
                delay = self._renew_delay(new_ttl)

            logger.trace(f"Vault token renewed, next renewal in {delay:.0f}s")

    # ....................... #

    def _create_client(self) -> Any:
        retry = Retry(
            total=self.config.retry_total,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=(412, 500, 502, 503, 504),
            raise_on_status=False,
        )
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))

        client = hvac.Client(
            url=self.config.url,
            token=self.config.token.get_secret_value(),
            namespace=self.config.namespace,
            verify=self.config.verify,
            session=session,
            timeout=int(self.config.timeout.total_seconds()),
        )

        if not client.is_authenticated():
            raise exc.infrastructure("Vault client is not authenticated")

        return client

    # ....................... #

    def _require_client(self) -> Any:
        if self._client is None:
            raise exc.infrastructure("Vault client is not initialized")

        return self._client

    # ....................... #

    def _read_kv_data_sync(self, path: str) -> JsonDict:
        client = self._require_client()

        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=self.config.mount_point,
            )

        except InvalidPath as e:
            raise exc.not_found(
                f"No secret for {path!r}",
                details={"ref": path},
            ) from e

        except VaultError as e:
            raise exc.infrastructure(f"Vault read failed for {path!r}: {e}") from e

        except Exception as e:
            raise exc.infrastructure(f"Vault read failed for {path!r}: {e}") from e

        data = response.get("data", {}).get("data")

        if not isinstance(data, dict):
            raise exc.infrastructure(
                f"Vault secret at {path!r} has unexpected payload shape",
            )

        return data  # type: ignore[return-value]

    # ....................... #

    async def read_kv_data(self, path: str) -> JsonDict:
        return await asyncio.to_thread(self._read_kv_data_sync, path)

    # ....................... #

    def _kv_exists_sync(self, path: str) -> bool:
        client = self._require_client()

        try:
            client.secrets.kv.v2.read_secret_metadata(
                path=path,
                mount_point=self.config.mount_point,
            )

            return True

        except InvalidPath:
            return False

        except VaultError as e:
            raise exc.infrastructure(
                f"Vault exists check failed for {path!r}: {e}"
            ) from e

        except Exception as e:
            raise exc.infrastructure(
                f"Vault exists check failed for {path!r}: {e}"
            ) from e

    # ....................... #

    async def kv_exists(self, path: str) -> bool:
        """Check secret existence via the KV v2 metadata endpoint.

        Uses metadata so the secret material is never read into memory.
        """

        return await asyncio.to_thread(self._kv_exists_sync, path)

    # ....................... #

    def _transit_generate_data_key_sync(self, key_name: str) -> tuple[bytes, str]:
        client = self._require_client()

        try:
            response = client.secrets.transit.generate_data_key(
                name=key_name,
                key_type="plaintext",
                mount_point=self.config.transit_mount,
            )

        except InvalidPath as e:
            raise exc.not_found(
                f"No Transit key {key_name!r}",
                details={"key": key_name},
            ) from e

        except VaultError as e:
            raise exc.infrastructure(
                f"Vault transit generate-data-key failed for {key_name!r}: {e}"
            ) from e

        except Exception as e:
            raise exc.infrastructure(
                f"Vault transit generate-data-key failed for {key_name!r}: {e}"
            ) from e

        data = response.get("data", {})
        plaintext_b64 = data.get("plaintext")
        ciphertext = data.get("ciphertext")

        if not isinstance(plaintext_b64, str) or not isinstance(ciphertext, str):
            raise exc.infrastructure(
                f"Vault transit data key for {key_name!r} has unexpected payload shape",
            )

        return base64.b64decode(plaintext_b64), ciphertext

    # ....................... #

    async def transit_generate_data_key(self, key_name: str) -> tuple[bytes, str]:
        return await asyncio.to_thread(self._transit_generate_data_key_sync, key_name)

    # ....................... #

    def _transit_decrypt_sync(self, key_name: str, ciphertext: str) -> bytes:
        client = self._require_client()

        try:
            response = client.secrets.transit.decrypt_data(
                name=key_name,
                ciphertext=ciphertext,
                mount_point=self.config.transit_mount,
            )

        except InvalidPath as e:
            raise exc.not_found(
                f"No Transit key {key_name!r}",
                details={"key": key_name},
            ) from e

        except VaultError as e:
            raise exc.infrastructure(
                f"Vault transit decrypt failed for {key_name!r}: {e}"
            ) from e

        except Exception as e:
            raise exc.infrastructure(
                f"Vault transit decrypt failed for {key_name!r}: {e}"
            ) from e

        plaintext_b64 = response.get("data", {}).get("plaintext")

        if not isinstance(plaintext_b64, str):
            raise exc.infrastructure(
                f"Vault transit decrypt for {key_name!r} has unexpected payload shape",
            )

        return base64.b64decode(plaintext_b64)

    # ....................... #

    async def transit_decrypt(self, key_name: str, ciphertext: str) -> bytes:
        return await asyncio.to_thread(
            self._transit_decrypt_sync, key_name, ciphertext
        )

    # ....................... #

    def _health_sync(self) -> tuple[str, bool]:
        client = self._require_client()
        status = client.sys.read_health_status(method="GET")

        if not isinstance(status, dict):  # requests.Response fallback
            ok = bool(getattr(status, "ok", False))
            return ("ok" if ok else f"status {status.status_code}", ok)

        status = cast(JsonDict, status)

        if status.get("sealed", False):
            return "sealed", False

        if not status.get("initialized", True):
            return "not initialized", False

        return "ok", True

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Report Vault health as ``(message, ok)``; never raises."""

        try:
            return await asyncio.to_thread(self._health_sync)

        except Exception as e:  # noqa: BLE001 - health must not raise
            return str(e), False
