"""Unit tests for :class:`forze_mongo.settings.MongoSettings` (no Mongo I/O).

The shared authority grammar — IPv6 brackets, blank-host refusal, port range — is proven
once against `EndpointSettings` in `test_forze/base/test_settings.py`. What is here is the
part only Mongo has: the SRV scheme, the credential prefix, and the option query.
"""

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("pymongo")

from forze_mongo.kernel.client import MongoConfig
from forze_mongo.settings import CLIENT_FIELDS, MongoSettings

# ----------------------- #


class TestUri:
    def test_builds_the_full_endpoint(self) -> None:
        settings = MongoSettings(
            host="m.internal",
            port=27017,
            user="app",
            password=SecretStr("hunter2"),
        )

        assert settings.uri.get_secret_value() == "mongodb://app:hunter2@m.internal:27017"

    # ....................... #

    def test_omits_credentials_when_neither_is_set(self) -> None:
        assert MongoSettings(host="m.internal").uri.get_secret_value() == "mongodb://m.internal"

    # ....................... #

    def test_a_password_without_a_user_is_refused(self) -> None:
        """The dangerous half: the URI would drop it and connect unauthenticated, so the
        failure is a successful connection with the wrong identity rather than an error."""

        with pytest.raises(ValidationError, match="password needs a user"):
            MongoSettings(host="m.internal", password=SecretStr("orphan"))

    # ....................... #

    def test_srv_selects_the_srv_scheme(self) -> None:
        settings = MongoSettings(host="cluster0.abc.mongodb.net", srv=True)

        assert settings.uri.get_secret_value().startswith("mongodb+srv://")

    # ....................... #

    def test_srv_refuses_a_port(self) -> None:
        """DNS supplies it, and pymongo rejects a URI that also names one."""

        with pytest.raises(ValidationError, match="leave port unset"):
            MongoSettings(host="cluster0.abc.mongodb.net", srv=True, port=27017)

    # ....................... #

    def test_options_become_the_query(self) -> None:
        settings = MongoSettings(
            host="m.internal",
            auth_source="admin",
            replica_set="rs0",
            tls=True,
        )

        assert settings.uri.get_secret_value().endswith(
            "/?authSource=admin&replicaSet=rs0&tls=true"
        )

    # ....................... #

    def test_no_options_means_no_query(self) -> None:
        assert MongoSettings(host="m.internal").uri.get_secret_value() == "mongodb://m.internal"

    # ....................... #

    def test_percent_encodes_credentials(self) -> None:
        settings = MongoSettings(host="m", user="a/b", password=SecretStr("p@ss"))

        assert settings.uri.get_secret_value() == "mongodb://a%2Fb:p%40ss@m"

    # ....................... #

    def test_requires_a_host(self) -> None:
        with pytest.raises(CoreException, match="Mongo host is required"):
            _ = MongoSettings().uri

    # ....................... #

    def test_a_seed_list_in_the_host_is_refused(self) -> None:
        """The docstring says one endpoint or one SRV record; a comma in the host would
        have made that true only where the entries happen to carry no port."""

        with pytest.raises(CoreException, match="must not contain"):
            _ = MongoSettings(host="a.internal,b.internal").uri


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert MongoSettings(host="m").config == MongoConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = MongoSettings(host="m", appname="orders-api", max_pool_size=25).config

        assert (config.appname, config.max_pool_size) == ("orders-api", 25)

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `MongoConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(MongoConfig)}
        assert set(CLIENT_FIELDS) <= set(MongoSettings.model_fields)
