"""Reading a callback: the order is the security property, so the order is what is tested."""

import pytest

from forze.base.exceptions import CoreException
from forze_identity.oauth import (
    CALLBACK_NO_CODE_CODE,
    CALLBACK_PROVIDER_ERROR_CODE,
    CALLBACK_STATE_MISMATCH_CODE,
    read_authorization_callback,
)

pytestmark = pytest.mark.unit

# ----------------------- #


def _code(raised: pytest.ExceptionInfo[CoreException]) -> str:
    return raised.value.code


# ....................... #


class TestTheHappyPath:
    def test_a_matching_state_yields_the_code(self) -> None:
        assert read_authorization_callback({"state": "s", "code": "c"}, expected_state="s") == "c"

    def test_unknown_parameters_are_ignored(self) -> None:
        # Providers append their own; the function reads what it needs and nothing else.
        code = read_authorization_callback(
            {"state": "s", "code": "c", "scope": "a b", "authuser": "0"}, expected_state="s"
        )

        assert code == "c"


class TestStateIsCheckedFirst:
    def test_a_mismatch_is_refused(self) -> None:
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"state": "other", "code": "c"}, expected_state="s")

        assert _code(raised) == CALLBACK_STATE_MISMATCH_CODE

    def test_an_absent_returned_state_is_refused(self) -> None:
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"code": "c"}, expected_state="s")

        assert _code(raised) == CALLBACK_STATE_MISMATCH_CODE

    def test_an_absent_session_state_is_refused(self) -> None:
        # This is the replay case: the callback was already consumed, so the session no
        # longer holds a state. An implementation that treated "nothing to compare" as
        # "nothing to check" would accept a replayed code.
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"state": "s", "code": "c"}, expected_state=None)

        assert _code(raised) == CALLBACK_STATE_MISMATCH_CODE

    def test_two_empty_states_do_not_match_each_other(self) -> None:
        # `compare_digest("", "")` is True, so an empty pair would pass a naive check —
        # which is exactly a callback arriving with no session at all.
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"state": ""}, expected_state="")

        assert _code(raised) == CALLBACK_STATE_MISMATCH_CODE

    def test_a_provider_error_does_not_bypass_the_state_check(self) -> None:
        # The order matters in both directions: an attacker who cannot forge the state must
        # not be able to steer the callback into the error branch either.
        with pytest.raises(CoreException) as raised:
            read_authorization_callback(
                {"state": "forged", "error": "access_denied"}, expected_state="s"
            )

        assert _code(raised) == CALLBACK_STATE_MISMATCH_CODE

    def test_the_refusal_says_nothing_about_the_values(self) -> None:
        # Nothing a caller can act on, and nothing an attacker can learn: not the expected
        # state, not the returned one, not which was missing.
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"state": "returned-value"}, expected_state="secret-state")

        message = str(raised.value)

        assert "secret-state" not in message
        assert "returned-value" not in message


class TestAProviderErrorIsAFailure:
    def test_access_denied_is_surfaced_not_exchanged(self) -> None:
        with pytest.raises(CoreException) as raised:
            read_authorization_callback(
                {"state": "s", "error": "access_denied"}, expected_state="s"
            )

        assert _code(raised) == CALLBACK_PROVIDER_ERROR_CODE
        assert (raised.value.details or {})["error"] == "access_denied"

    def test_an_error_wins_over_a_code_that_came_with_it(self) -> None:
        # A callback carrying both is not a success with a note attached.
        with pytest.raises(CoreException) as raised:
            read_authorization_callback(
                {"state": "s", "error": "consent_required", "code": "c"}, expected_state="s"
            )

        assert _code(raised) == CALLBACK_PROVIDER_ERROR_CODE

    def test_the_provider_description_is_not_carried(self) -> None:
        # Provider-authored text, reflected back into an exception nobody sanitised.
        with pytest.raises(CoreException) as raised:
            read_authorization_callback(
                {
                    "state": "s",
                    "error": "access_denied",
                    "error_description": "<script>alert(1)</script>",
                },
                expected_state="s",
            )

        assert "script" not in repr(raised.value.details)


class TestACodeMustBePresent:
    def test_neither_code_nor_error_is_refused(self) -> None:
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"state": "s"}, expected_state="s")

        assert _code(raised) == CALLBACK_NO_CODE_CODE

    def test_an_empty_code_is_refused(self) -> None:
        with pytest.raises(CoreException) as raised:
            read_authorization_callback({"state": "s", "code": ""}, expected_state="s")

        assert _code(raised) == CALLBACK_NO_CODE_CODE
