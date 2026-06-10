"""HashiCorp Vault KV v2 client backed by ``hvac``."""

from forze_vault._compat import require_vault

require_vault()

# ....................... #

import asyncio
from typing import Any, final

import attrs
import hvac
import requests
from hvac.exceptions import InvalidPath, VaultError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .port import VaultClientPort
from .value_objects import VaultConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class VaultClient(VaultClientPort):
    """Async-friendly Vault KV v2 reader using a blocking ``hvac`` client in a thread pool."""

    config: VaultConfig
    _client: Any = attrs.field(default=None, init=False, repr=False)
    _init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(self) -> None:
        async with self._init_lock:
            if self._client is not None:
                return

            self._client = await asyncio.to_thread(self._create_client)

    # ....................... #

    async def close(self) -> None:
        async with self._init_lock:
            self._client = None

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
            client.secrets.kv.v2.read_secret_version(
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
        return await asyncio.to_thread(self._kv_exists_sync, path)
