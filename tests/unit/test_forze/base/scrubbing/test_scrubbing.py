"""Unit tests for forze.base.scrubbing."""

from collections.abc import Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, EmailStr, SecretStr, ValidationError

from tests.support.hypothesis_strategies import integration_hypothesis_settings

from forze.base.scrubbing import (
    SECRET_PLACEHOLDER,
    dump_bound_args_for_errors,
    dump_for_error_context,
    register_sensitive_patterns,
    sanitize,
    sanitize_pydantic_errors,
)
from forze.base.scrubbing import policy as _scrub_policy
from forze.base.scrubbing.policy import scrub_log_string

# ----------------------- #


class _SecretModel(BaseModel):
    password: str
    api_key: SecretStr


class TestSanitizeEgress:
    def test_secret_str_masked(self) -> None:
        assert sanitize(SecretStr("hunter2"), context="egress") == SECRET_PLACEHOLDER

    def test_nested_mapping_sensitive_key(self) -> None:
        data = {"user": {"password": "hunter2"}, "id": "1"}
        assert sanitize(data, context="egress") == {
            "user": {"password": SECRET_PLACEHOLDER},
            "id": "1",
        }

    def test_egress_does_not_scrub_email_in_note(self) -> None:
        data = {"note": "contact alice@example.com"}
        assert sanitize(data, context="egress") == data


class TestSanitizeLog:
    def test_masks_sensitive_keys(self) -> None:
        data = {"password": "hunter2", "id": "1"}
        assert sanitize(data, context="log") == {
            "password": SECRET_PLACEHOLDER,
            "id": "1",
        }

    def test_scrubs_email_in_string_when_text_scrub_enabled(self) -> None:
        data = {"note": "contact alice@example.com"}
        result = sanitize(data, context="log", text_scrub=True)
        assert "alice@example.com" not in str(result["note"])
        assert SECRET_PLACEHOLDER in result["note"]

    def test_scrubs_bearer_in_note(self) -> None:
        data = {"note": "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9"}
        result = sanitize(data, context="log", text_scrub=True)
        assert "eyJ" not in result["note"]
        assert SECRET_PLACEHOLDER in result["note"]

    def test_scrubs_password_substring_in_note(self) -> None:
        data = {"note": "failed: password=hunter2"}
        result = sanitize(data, context="log", text_scrub=True)
        assert "hunter2" not in result["note"]
        assert SECRET_PLACEHOLDER in result["note"]

    def test_session_in_key_masks_whole_value(self) -> None:
        data = {"session_id": "abc"}
        assert sanitize(data, context="log") == {"session_id": SECRET_PLACEHOLDER}

    def test_text_scrub_can_be_disabled(self) -> None:
        data = {"note": "contact alice@example.com"}
        assert sanitize(data, context="log", text_scrub=False) == data

    def test_scrubs_postgresql_dsn_in_string(self) -> None:
        text = "connect failed: postgresql://user:secret@db.example.com:5432/app"
        result = scrub_log_string(text)
        assert "postgresql://" not in result
        assert SECRET_PLACEHOLDER in result

    def test_scrubs_inline_private_key_json_fragment(self) -> None:
        text = 'config {"private_key": "-----BEGIN PRIVATE KEY-----\\nabc"}'
        result = scrub_log_string(text)
        assert "-----BEGIN PRIVATE KEY-----" not in result
        assert SECRET_PLACEHOLDER in result

    def test_does_not_corrupt_endpoint_paths(self) -> None:
        # a bare sensitive word as a path segment is NOT a secret value — keep it
        for path in ("/v1/authn/login", "/oauth/callback", "/storage/credentials/list"):
            assert scrub_log_string(path) == path
            assert SECRET_PLACEHOLDER not in scrub_log_string(f"GET {path}")

    def test_still_masks_secret_query_parameter_value(self) -> None:
        # the value-bearing shape (key=value) is what carries the secret
        result = scrub_log_string("GET /v1/authn/login?session=abc123&token=xyz")
        assert "abc123" not in result
        assert "xyz" not in result
        assert "/v1/authn/login" in result  # the path is left intact


class TestScrubAssignmentSuffixLeak:
    """Compound sensitive names (term + suffix before ``=``/``:``) must be masked."""

    @pytest.mark.parametrize(
        "text",
        [
            "using secret_key=abc123",
            "env aws_secret_access_key=AKIAWEAKKEY123",
            "with token_value=xyz789",
            "cfg client_secret=shhh",  # already covered pre-fix; must stay covered
            "hdr api_key_id=leakme",
            "sess session_token=deadbeef",
        ],
    )
    def test_masks_compound_assignment(self, text: str) -> None:
        result = scrub_log_string(text)
        assert SECRET_PLACEHOLDER in result
        # The value after the separator must be gone.
        for leaked in (
            "abc123",
            "AKIAWEAKKEY123",
            "xyz789",
            "shhh",
            "leakme",
            "deadbeef",
        ):
            if leaked in text:
                assert leaked not in result

    def test_masks_authorization_header_any_scheme(self) -> None:
        result = scrub_log_string("Authorization: Basic dXNlcjpwYXNz")
        assert "dXNlcjpwYXNz" not in result
        assert SECRET_PLACEHOLDER in result

    @pytest.mark.parametrize(
        ("text", "secret"),
        [
            ("olap clickhouse://user:hunter2@ch:9000/db", "hunter2"),
            ("store mongodb://admin:s3cr3t@mongo:27017/app", "s3cr3t"),
            ("fetch https://alice:topsecret@api.example.com/v1", "topsecret"),
            ("srv mongodb+srv://u:p4ss@cluster0.mongodb.net", "p4ss"),
        ],
    )
    def test_masks_scheme_agnostic_userinfo(self, text: str, secret: str) -> None:
        result = scrub_log_string(text)
        assert secret not in result
        assert SECRET_PLACEHOLDER in result

    @pytest.mark.parametrize(
        "text",
        [
            "GET /v1/orders/42 completed",
            "the session expired after 30 minutes",
            "visit https://example.com/docs for details",  # no userinfo, no secret
            "user authorized the request",  # 'authorization' not followed by ':'
            "order 12345 fulfilled for customer 9876",
            # A bare word continuation of a sensitive term must NOT be swallowed: the suffix
            # only extends across separator-led segments, so these stay ordinary text.
            "secretary=Jane started today",
            "the tokenizer=bpe finished",
            "sessionization=on in the config",
        ],
    )
    def test_leaves_non_secret_strings_untouched(self, text: str) -> None:
        assert scrub_log_string(text) == text


class TestCredentialFragmentCoverage:
    """Credential terms must be caught on every scrub path (keys, messages, exceptions)."""

    @pytest.mark.parametrize(
        ("text", "secret"),
        [
            ("loading private_key=abc for signing", "abc"),
            ("login with pwd=zzz failed", "zzz"),
            ("db_pwd=hunter2 in env", "hunter2"),
            ("wallet passphrase=correcthorse rejected", "correcthorse"),
            ("wallet passphrase: correcthorse rejected", "correcthorse"),
            ("kms private-key=abc rotated", "abc"),
            ("card credit_card=4111111111111111 declined", "4111111111111111"),
            ("payload social_security=078-05-1120 stripped", "078-05-1120"),
            ("auth=abc123 attached to request", "abc123"),
            ("connect dsn=Server=db;Uid=sa;Pwd=x1 failed", "Pwd=x1"),
        ],
    )
    def test_masks_assignment_form_in_message(self, text: str, secret: str) -> None:
        result = scrub_log_string(text)
        assert secret not in result
        assert SECRET_PLACEHOLDER in result

    @pytest.mark.parametrize(
        "key",
        [
            "pwd",
            "db_pwd",
            "pwd_hash",
            "mysql pwd",
            "passphrase",
            "wallet_passphrase",
            "private_key",
        ],
    )
    def test_masks_structured_key(self, key: str) -> None:
        assert sanitize({key: "leak"}, context="log") == {key: SECRET_PLACEHOLDER}

    @pytest.mark.parametrize(
        "key",
        [
            "pwdx",  # 'pwd' not underscore/word-boundary delimited on the right
            "backupwd",  # 'pwd' mid-token, not delimited on the left
            "crowd",
        ],
    )
    def test_short_fragment_key_anchoring_has_no_false_positives(
        self, key: str
    ) -> None:
        assert sanitize({key: "v"}, context="log") == {key: "v"}

    @pytest.mark.parametrize(
        "text",
        [
            "the pwd command prints the working directory",  # no ``=``/``:`` + value shape
            "passphrases are long by design",
            "security=high for this tenant",  # 'uri' inside 'security' must not trigger
            "user authorized the request",
            "the author: Jane Doe",  # 'auth' lookahead excludes author/authors
        ],
    )
    def test_assignment_form_has_no_false_positives(self, text: str) -> None:
        assert scrub_log_string(text) == text

    def test_masks_exception_message_and_stack_path(self) -> None:
        from forze.base.logging.constants import ERR_MESSAGE_KEY, ERR_STACK_KEY
        from forze.base.logging.processors import ExceptionFieldsSanitizer

        event_dict = {
            ERR_MESSAGE_KEY: "connect failed: private_key=abc pwd=zzz",
            ERR_STACK_KEY: "  File x, line 1\n    conn(db_pwd=hunter2)",
        }
        result = ExceptionFieldsSanitizer()(None, "", event_dict)

        for leaked in ("abc", "zzz", "hunter2"):
            assert leaked not in result[ERR_MESSAGE_KEY] + result[ERR_STACK_KEY]

    def test_masks_rendered_event_message_path(self) -> None:
        from forze.base.logging.processors import EventDictSanitizer

        event_dict = {
            "event": "retry with pwd=zzz and private_key=abc",
            "passphrase": "correcthorse",
            "level": "info",
        }
        result = EventDictSanitizer()(None, "", event_dict)

        assert "zzz" not in result["event"]
        assert "abc" not in result["event"]
        assert result["passphrase"] == SECRET_PLACEHOLDER


class TestFragmentListParity:
    """Drift guard: the key-heuristic and value-form term lists must stay reconciled.

    The sensitive-key heuristic (``is_sensitive_key``) and the value-form
    assignment rule (``foo=secret`` in message strings) are two projections of
    one credential vocabulary. A term added to only one of them silently leaks
    on the other path, so this test fails when the lists diverge beyond the
    explicit exception sets below.
    """

    # Key-heuristic terms with no value-form assignment counterpart, with the
    # reason the value form is handled elsewhere (or inapplicable).
    _KEY_ONLY_TERMS = {
        # The value form is owned by the dedicated full-line
        # ``authorization\s*:\s*[^\r\n]+`` extras pattern: an assignment-term
        # match would stop at the first token (the scheme word, e.g. ``Basic``)
        # and leak the credential that follows it.
        "authorization",
    }

    # Value-form terms with no key-heuristic counterpart.
    _VALUE_ONLY_TERMS: set[str] = set()

    @staticmethod
    def _canonical_term(fragment: str) -> str:
        """Collapse a regex fragment to a comparable bare term."""

        term = fragment.lower()

        for regex_noise in (r"(?:\b|_)", r"(?!ors?\b)", r"[._ -]?"):
            term = term.replace(regex_noise, "")

        term = term.replace("_", "")

        assert term.isalnum(), (
            f"fragment {fragment!r} uses a regex construct _canonical_term does not"
            f" normalize; teach it the construct so parity stays checkable"
        )
        return term

    def test_key_and_value_term_lists_are_reconciled(self) -> None:
        from forze.base.scrubbing import policy

        key_terms = {
            self._canonical_term(fragment)
            for fragment in (
                *policy._LOGFIRE_SENSITIVE_FRAGMENTS,
                *policy._FORZE_KEY_EXTRAS,
            )
        }
        value_terms = {
            self._canonical_term(fragment)
            for fragment in policy._LOG_ASSIGNMENT_TERM_FRAGMENTS
        }

        missing_from_value = key_terms - value_terms
        missing_from_key = value_terms - key_terms

        assert missing_from_value == self._KEY_ONLY_TERMS, (
            "key-heuristic terms missing a value-form assignment counterpart;"
            " add them to _LOG_ASSIGNMENT_TERM_FRAGMENTS or document the"
            f" exception here: {sorted(missing_from_value - self._KEY_ONLY_TERMS)}"
        )
        assert missing_from_key == self._VALUE_ONLY_TERMS, (
            "value-form terms missing a key-heuristic counterpart; add them to"
            " _FORZE_KEY_EXTRAS or document the exception here:"
            f" {sorted(missing_from_key - self._VALUE_ONLY_TERMS)}"
        )

    def test_key_only_exception_still_masks_its_value_form(self) -> None:
        # The documented exception is only valid while the full-line pattern
        # actually owns the authorization value form.
        result = scrub_log_string("Authorization: Basic dXNlcjpwYXNz")
        assert "dXNlcjpwYXNz" not in result
        assert "Basic" not in result


class TestSanitizeNonStrKey:
    """A non-str mapping key must never crash the scrubber (Bug 3)."""

    def test_int_key_does_not_raise(self) -> None:
        data = {"stats": {1: 2, 3: 4}}
        result = sanitize(data, context="log")
        assert result == {"stats": {1: 2, 3: 4}}

    def test_mixed_keys_with_sensitive(self) -> None:
        data = {1: "a", "password": "hunter2", (2, 3): "b"}
        result = sanitize(data, context="log")
        assert result[1] == "a"
        assert result["password"] == SECRET_PLACEHOLDER
        assert result[(2, 3)] == "b"

    def test_non_str_key_named_like_secret_is_masked(self) -> None:
        # str(key) is inspected: an object whose repr matches the heuristic masks.
        class _Token:
            def __str__(self) -> str:
                return "token"

        key = _Token()
        result = sanitize({key: "leak"}, context="log")
        assert result[key] == SECRET_PLACEHOLDER


class TestSanitizePydanticErrors:
    def test_strips_input_and_ctx(self) -> None:
        class M(BaseModel):
            email: EmailStr

        with pytest.raises(ValidationError) as exc_info:
            M.model_validate({"email": "not-an-email"})

        sanitized = sanitize_pydantic_errors(exc_info.value.errors())
        assert sanitized
        assert "input" not in sanitized[0]
        assert "ctx" not in sanitized[0]
        assert "loc" in sanitized[0]
        assert "msg" in sanitized[0]


class TestDumpForErrorContext:
    def test_masks_secret_str_and_plain_password_field(self) -> None:
        model = _SecretModel(password="plain-secret", api_key=SecretStr("key-secret"))
        dumped = dump_for_error_context(model)
        assert dumped["password"] == SECRET_PLACEHOLDER
        assert dumped["api_key"] == SECRET_PLACEHOLDER


class TestDumpBoundArgsForErrors:
    def test_dumps_base_model_args(self) -> None:
        model = _SecretModel(password="x", api_key=SecretStr("y"))
        ctx = dump_bound_args_for_errors({"dto": model, "limit": 10})
        assert ctx["limit"] == 10
        assert ctx["dto"]["password"] == SECRET_PLACEHOLDER


@integration_hypothesis_settings
@given(
    key=st.text(
        min_size=1, max_size=12, alphabet=st.characters(blacklist_categories=("Cs",))
    ),
    value=st.text(min_size=0, max_size=24),
)
def test_sanitize_log_masks_nested_sensitive_keys(key: str, value: str) -> None:
    sensitive_key = f"user_{key}_password"
    data = {"outer": {sensitive_key: value, "safe": "ok"}}
    result = sanitize(data, context="log")
    assert result["outer"]["safe"] == "ok"
    assert result["outer"][sensitive_key] == SECRET_PLACEHOLDER


class TestRegisterSensitivePatterns:
    @pytest.fixture(autouse=True)
    def _restore_policy(self) -> Iterator[None]:
        from forze.base.scrubbing import policy

        keys = list(policy._EXTRA_SENSITIVE_KEY_PATTERNS)
        logs = list(policy._EXTRA_LOG_STRING_PATTERNS)

        try:
            yield

        finally:
            policy._EXTRA_SENSITIVE_KEY_PATTERNS[:] = keys
            policy._EXTRA_LOG_STRING_PATTERNS[:] = logs
            # Rebuild through the chokepoint so the key memo and the
            # prefilter literals are regenerated, not just the regexes.
            policy._rebuild_matchers()

    def test_custom_key_pattern_masks_value(self) -> None:
        field = "acme_widget_handle"

        assert sanitize({field: "v"}, context="log") == {field: "v"}

        register_sensitive_patterns(keys=[r"widget[._ -]?handle"])

        assert sanitize({field: "v"}, context="log") == {field: SECRET_PLACEHOLDER}

    def test_custom_log_string_pattern_is_scrubbed(self) -> None:
        text = "issued ACME-TOKEN-abc123"

        assert scrub_log_string(text) == text

        register_sensitive_patterns(log_strings=[r"ACME-TOKEN-\S+"])
        result = scrub_log_string(text)

        assert "abc123" not in result
        assert SECRET_PLACEHOLDER in result

    def test_empty_patterns_are_ignored(self) -> None:
        register_sensitive_patterns(keys=[""], log_strings=[""])

        # An empty fragment would otherwise match everything; it must be dropped.
        assert sanitize({"plain_field": "v"}, context="log") == {"plain_field": "v"}
        assert scrub_log_string("nothing sensitive here") == "nothing sensitive here"

    def test_key_cache_invalidated_on_register(self) -> None:
        from forze.base.scrubbing.policy import is_sensitive_key

        field = "acme_widget_handle"

        # Prime the memo with a negative answer, twice (second call is a cache hit).
        assert is_sensitive_key(field) is False
        assert is_sensitive_key(field) is False

        register_sensitive_patterns(keys=[r"widget[._ -]?handle"])

        # The mutator must clear the memo, or the stale False would leak secrets.
        assert is_sensitive_key(field) is True

    def test_pattern_without_literal_disables_prefilter_but_scrubs(self) -> None:
        from forze.base.scrubbing import policy

        # Built-in patterns all contribute literals: prefilter starts active.
        assert policy._log_string_literals is not None

        # A raw digit-run pattern has no required literal substring.
        register_sensitive_patterns(log_strings=[r"\d{16}"])

        assert policy._log_string_literals is None  # prefilter disabled for safety
        result = scrub_log_string("card 1234567812345678 charged")
        assert "1234567812345678" not in result
        assert SECRET_PLACEHOLDER in result
        # No-match strings still pass through unchanged (just slower).
        assert scrub_log_string("plain order message") == "plain order message"

    def test_custom_literal_pattern_keeps_prefilter_active(self) -> None:
        from forze.base.scrubbing import policy

        register_sensitive_patterns(log_strings=[r"ACME-TOKEN-\S+"])

        literals = policy._log_string_literals
        assert literals is not None
        assert any(lit in "acme-token-" for lit in literals)
        assert SECRET_PLACEHOLDER in scrub_log_string("issued ACME-TOKEN-abc123")


# ----------------------- #
# Prefilter supersetness proof
# ----------------------- #

# One matching sample per built-in log-string fragment. The exhaustiveness test
# below fails when a fragment is added without a sample here.
_FRAGMENT_SAMPLES: dict[str, str] = {
    # The assignment fragment is assembled from _LOG_ASSIGNMENT_TERM_FRAGMENTS;
    # per-term coverage lives in TestCredentialFragmentCoverage and the parity
    # guard, so one representative sample suffices here.
    _scrub_policy._LOG_ASSIGNMENT_FRAGMENTS[0]: "retry with api key=abc123",
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}": "mail sent to alice@example.com today",
    r"Bearer\s+\S+": "header was Bearer eyJhbGci.x.y",
    r"authorization\s*:\s*[^\r\n]+": "Authorization: Basic dXNlcjpwYXNz",
    r"postgresql(?:\+[a-z]+)?://\S+": "dsn postgresql+asyncpg://u:p@db:5432/app",
    r"mysql(?:\+[a-z]+)?://\S+": "dsn mysql://u:p@db:3306/app",
    r"redis(?:\+[a-z]+)?://\S+": "cache at redis://cache:6379/0",
    r"amqps?://\S+": "broker amqps://guest:guest@mq:5671/",
    r"\w[\w+.-]*://[^\s/@:]+:[^\s@]+@": "olap clickhouse://user:pass@ch:9000/db",
    r'"private_key"\s*:\s*"[^"]*"': 'cfg {"private_key": "-----BEGIN-----"}',
}


class TestPrefilterSupersetness:
    """The literal prefilter must never skip a string the combined regex scrubs."""

    def test_every_builtin_fragment_has_a_sample(self) -> None:
        from forze.base.scrubbing import policy

        # The value regex uses assignment + extras only — the bare Logfire
        # key-name fragments are for is_sensitive_key, not value scrubbing.
        builtin = {
            *policy._LOG_ASSIGNMENT_FRAGMENTS,
            *policy._LOG_STRING_EXTRAS,
        }
        assert builtin == set(_FRAGMENT_SAMPLES)

    def test_prefilter_is_active_by_default(self) -> None:
        from forze.base.scrubbing import policy

        assert policy._log_string_literals is not None

    @pytest.mark.parametrize(
        ("fragment", "sample"),
        sorted(_FRAGMENT_SAMPLES.items()),
        ids=lambda v: repr(v)[:40],
    )
    def test_prefilter_passes_every_matching_sample(
        self, fragment: str, sample: str
    ) -> None:
        import re

        from forze.base.scrubbing import policy

        # The sample really matches this individual fragment...
        assert re.compile(fragment, policy._SCRUB_FLAGS).search(sample), fragment

        # ...the prefilter does not reject it (a literal is present)...
        literals = policy._log_string_literals
        assert literals is not None
        lowered = sample.lower()
        assert any(lit in lowered for lit in literals), (fragment, sample)

        # ...and end-to-end scrubbing through the prefilter masks it.
        result = scrub_log_string(sample)
        assert result != sample
        assert SECRET_PLACEHOLDER in result


class TestScrubPrefilterBehaviorIdentical:
    """Prefiltered scrub_log_string is byte-identical to the raw combined regex."""

    _CORPUS = (
        "order 12345 fulfilled for customer 9876 in region eu-west-1",
        "call back at +1 (555) 010-2030 tomorrow",  # phone: no pattern, untouched
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig",
        "connect failed: postgresql://user:hunter2@db.example.com:5432/app",
        "cache: redis://:p4ss@cache.internal:6379/2",
        "broker amqp://guest:guest@mq:5672/vhost",
        "notify alice@example.com and bob.smith+tag@sub.example.org",
        "login failed: password=hunter2 attempt=3",
        'loaded {"private_key": "-----BEGIN PRIVATE KEY-----"}',
        "the jwt expired; csrf token mismatch",
        "plain message with no sensitive content at all",
        "",
    )

    @pytest.mark.parametrize("text", _CORPUS, ids=lambda t: t[:32] or "<empty>")
    def test_matches_raw_regex_substitution(self, text: str) -> None:
        from forze.base.scrubbing import policy

        assert scrub_log_string(text) == policy._log_string_re.sub(
            SECRET_PLACEHOLDER, text
        )

    def test_no_match_strings_returned_unchanged(self) -> None:
        msg = "order 12345 fulfilled for customer 9876"
        assert scrub_log_string(msg) == msg


# ....................... #


class TestWalkBranches:
    """Direct coverage of the recursive scrub walk's value/mapping branches."""

    def _walk(self):
        from forze.base.scrubbing._walk import walk_mapping, walk_value

        return walk_value, walk_mapping

    def test_walk_value_max_depth_returns_sentinel(self) -> None:
        from forze.base.scrubbing.policy import MAX_DEPTH_SENTINEL

        walk_value, _ = self._walk()
        assert (
            walk_value({"a": 1}, text_scrub=False, depth=5, max_depth=4)
            == MAX_DEPTH_SENTINEL
        )

    def test_walk_mapping_max_depth_returns_sentinel(self) -> None:
        from forze.base.scrubbing.policy import MAX_DEPTH_SENTINEL

        _, walk_mapping = self._walk()
        assert walk_mapping({"a": 1}, text_scrub=False, depth=5, max_depth=4) == {
            MAX_DEPTH_SENTINEL: True
        }

    def test_walk_value_basemodel_is_dumped_and_key_masked(self) -> None:
        class _M(BaseModel):
            secret: str = "x"
            name: str = "ok"

        walk_value, _ = self._walk()
        out = walk_value(_M(), text_scrub=False, depth=0, max_depth=8)
        assert out["secret"] == SECRET_PLACEHOLDER  # sensitive key name masked
        assert out["name"] == "ok"

    def test_walk_value_bytes_pass_through_untouched(self) -> None:
        walk_value, _ = self._walk()
        raw = b"binary-blob"
        assert walk_value(raw, text_scrub=True, depth=0, max_depth=8) is raw

    def test_walk_value_sequence_recurses_into_items(self) -> None:
        walk_value, _ = self._walk()
        out = walk_value(
            ["plain", {"secret": "s"}], text_scrub=False, depth=0, max_depth=8
        )
        assert out[0] == "plain"
        assert out[1]["secret"] == SECRET_PLACEHOLDER

    def test_walk_mapping_masks_key_whose_str_raises(self) -> None:
        class _BadKey:
            __hash__ = object.__hash__

            def __str__(self) -> str:
                raise RuntimeError("boom")

        _, walk_mapping = self._walk()
        bad = _BadKey()
        out = walk_mapping({bad: "value"}, text_scrub=False, depth=0, max_depth=8)
        # A key that cannot be stringified is masked, never propagated.
        assert out[bad] == SECRET_PLACEHOLDER
