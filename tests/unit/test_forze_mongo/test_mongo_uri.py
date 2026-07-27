"""Unit tests for the MongoDB URI credential helpers used by rotation.

Rotation swaps the userinfo of a connection URI in place, so these have to preserve
everything else exactly: multiple hosts, the replica-set and auth query options, the
``mongodb+srv://`` scheme, and percent-encoded credentials. Getting any of that wrong
produces a URI that still *parses* and connects to the wrong place — or not at all, mid
rotation, which is the worst moment for it.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_mongo.kernel.uri import (
    mongo_uri_password,
    mongo_uri_username,
    with_mongo_credentials,
)

# ----------------------- #

_SIMPLE = "mongodb://app:secret@localhost:27017/admin"
_MULTI_HOST = "mongodb://app:secret@h1:27017,h2:27017,h3:27017/admin?replicaSet=rs0&tls=true"
_SRV = "mongodb+srv://app:secret@cluster.example.net/admin?retryWrites=true"


class TestReadingCredentials:
    @pytest.mark.parametrize("uri", [_SIMPLE, _MULTI_HOST, _SRV])
    def test_username_and_password_round_trip(self, uri: str) -> None:
        assert mongo_uri_username(uri) == "app"
        assert mongo_uri_password(uri) == "secret"

    def test_percent_encoded_credentials_are_decoded(self) -> None:
        """The URI carries them encoded; a caller comparing against the real password —
        which is what verify-before-promote does — needs the decoded value."""

        uri = "mongodb://us%40er:p%40ss%2Fword@localhost:27017/admin"

        assert mongo_uri_username(uri) == "us@er"
        assert mongo_uri_password(uri) == "p@ss/word"

    def test_a_uri_naming_no_user_is_refused(self) -> None:
        """Rotation is credential-swapping; a URI with nothing to swap is a wiring error,
        not something to paper over with an empty username."""

        with pytest.raises(CoreException) as ei:
            mongo_uri_username("mongodb://localhost:27017/admin")

        assert ei.value.kind == ExceptionKind.CONFIGURATION


class TestSwappingCredentials:
    def test_hosts_options_and_scheme_survive_a_swap(self) -> None:
        swapped = with_mongo_credentials(_MULTI_HOST, username="app_b", password="next")

        assert mongo_uri_username(swapped) == "app_b"
        assert mongo_uri_password(swapped) == "next"
        # Everything that is not userinfo is untouched.
        assert "h1:27017,h2:27017,h3:27017" in swapped
        assert "replicaSet=rs0" in swapped
        assert "tls=true" in swapped
        assert swapped.startswith("mongodb://")

    def test_the_srv_scheme_is_preserved(self) -> None:
        """``mongodb+srv`` changes host resolution entirely — dropping the suffix would
        point the rotated connection at a hostname that does not resolve."""

        swapped = with_mongo_credentials(_SRV, username="app_b", password="next")

        assert swapped.startswith("mongodb+srv://")
        assert "cluster.example.net" in swapped
        assert mongo_uri_username(swapped) == "app_b"

    def test_special_characters_are_encoded_on_the_way_in(self) -> None:
        """A generated password containing ``@`` or ``/`` must not split the authority."""

        swapped = with_mongo_credentials(_SIMPLE, username="us@er", password="p@ss/word")

        # Round-trips through the reader, which is the property that matters.
        assert mongo_uri_username(swapped) == "us@er"
        assert mongo_uri_password(swapped) == "p@ss/word"
        # And the host survived — the raw '@' would otherwise have re-split the authority.
        assert "localhost:27017" in swapped
