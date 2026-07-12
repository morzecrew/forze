"""Sensitive-key and log-string scrub policy for structured payloads.

Log-context string rules follow Logfire default scrubbing patterns (substring
matches). Innocent words in log messages (e.g. "session expired") may be
redacted; use ``text_scrub=False`` on :func:`~forze.base.scrubbing.sanitize` or
:func:`~forze.base.logging.configure.configure_logging` to disable.
"""

import re

# The regex parse tree is the only reliable way to derive required literals for
# the scrub prefilter; these internal modules are stable across 3.11+ but ship
# without stubs.
import re._constants as _sre_constants  # type: ignore[import-not-found]
import re._parser as _sre_parse  # type: ignore[import-not-found]
from collections.abc import Sequence
from functools import lru_cache
from typing import Any

# ----------------------- #

SECRET_PLACEHOLDER: str = "**********"
"""Mask string aligned with Pydantic :class:`~pydantic.SecretStr` JSON serialization."""

DEFAULT_MAX_DEPTH: int = 8
"""Default maximum nesting depth for :func:`~forze.base.scrubbing.sanitize`."""

MAX_DEPTH_SENTINEL: str = "<max_depth>"

# Logfire DEFAULT_PATTERNS (https://github.com/pydantic/logfire/blob/main/logfire/_internal/scrubbing.py)
_LOGFIRE_SENSITIVE_FRAGMENTS: tuple[str, ...] = (
    "password",
    "passwd",
    "mysql_pwd",
    "secret",
    r"auth(?!ors?\b)",
    "credential",
    "private[._ -]?key",
    "api[._ -]?key",
    "session",
    "cookie",
    "social[._ -]?security",
    "credit[._ -]?card",
    "logfire[._ -]?token",
    r"(?:\b|_)csrf(?:\b|_)",
    r"(?:\b|_)xsrf(?:\b|_)",
    r"(?:\b|_)jwt(?:\b|_)",
    r"(?:\b|_)ssn(?:\b|_)",
)

# Extra key terms for egress/log key masking (not all appear in Logfire defaults).
# Short fragments are anchored ``(?:\b|_)…(?:\b|_)`` (the csrf/jwt convention above)
# so ``pwd``/``db_pwd``/``pwd_hash`` match but a mid-token run (``backupwd``) does not.
_FORZE_KEY_EXTRAS: tuple[str, ...] = (
    "token",
    "dsn",
    "uri",
    "authorization",
    r"(?:\b|_)pwd(?:\b|_)",
    "passphrase",
)

# Log-context string rules only (assignments, email, Bearer tokens).
#
# Value scrubbing must match a secret-bearing *shape*, never a bare word: the
# Logfire fragments above are key-name heuristics (for ``is_sensitive_key``) and
# matching them inside arbitrary string content corrupts ordinary text — a path
# like ``/v1/authn/login`` or a message mentioning "session" — while still leaking
# the real value next to the masked word. So a sensitive term scrubs a log value
# only when it is followed by ``=``/``:`` and a token (``session=abc`` → masked
# whole), which is the form that actually carries the secret.
#
# A bounded compound-name suffix (``(?:[._-]\w+){0,6}``) sits between the sensitive term
# and the ``=``/``:`` separator so compound names carry through: ``secret_key=``,
# ``aws_secret_access_key=``, ``token_value=`` all match (previously only a term
# immediately followed by the separator — e.g. ``client_secret=`` — was caught). Each suffix
# segment must be separator-led (``_``/``.``/``-`` then word chars), so a bare word
# continuation is *not* swallowed: ``secretary=`` / ``tokenizer=`` stay ordinary text.
#
# This vocabulary is the value-form projection of the sensitive-key terms above
# (Logfire fragments + Forze key extras): every key term has an assignment
# counterpart here and vice versa, so a credential is masked whether it appears
# as an event-dict key or inline in a message string. The one deliberate
# key-only term is ``authorization``, whose value form is owned by the
# full-line ``authorization\s*:`` rule below — an assignment match would stop
# at the scheme word (``Basic``) and leak the credential after it. A parity
# test enforces the reconciliation.
_LOG_ASSIGNMENT_TERM_FRAGMENTS: tuple[str, ...] = (
    "password",
    "passwd",
    r"mysql[._ -]?pwd",
    # Left-bounded like its key-heuristic twin: ``pwd=`` / ``db_pwd=`` match,
    # a mid-token run (``backupwd=``) stays ordinary text.
    r"(?:\b|_)pwd",
    "passphrase",
    "secret",
    "token",
    r"logfire[._ -]?token",
    r"api[._ -]?key",
    r"private[._ -]?key",
    r"auth(?!ors?\b)",
    "credential",
    "session",
    "cookie",
    r"social[._ -]?security",
    r"credit[._ -]?card",
    "csrf",
    "xsrf",
    "jwt",
    "ssn",
    "dsn",
    "uri",
)

_LOG_ASSIGNMENT_FRAGMENTS: tuple[str, ...] = (
    "(?:"
    + "|".join(_LOG_ASSIGNMENT_TERM_FRAGMENTS)
    + r")(?:[._-]\w+){0,6}\s*[=:]\s*\S+",
)

_LOG_STRING_EXTRAS: tuple[str, ...] = (
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    r"Bearer\s+\S+",
    # Any ``Authorization: <credential>`` header value, not just the Bearer
    # scheme; consume the rest of the line so ``Basic <b64>`` / ``Bearer <jwt>``
    # (scheme + credential token) are masked whole, not just the scheme word.
    r"authorization\s*:\s*[^\r\n]+",
    r"postgresql(?:\+[a-z]+)?://\S+",
    r"mysql(?:\+[a-z]+)?://\S+",
    r"redis(?:\+[a-z]+)?://\S+",
    r"amqps?://\S+",
    # Scheme-agnostic ``scheme://user:pass@`` userinfo, so clickhouse://, mongodb://,
    # https://user:pass@ … DSNs are masked, not only the four schemes enumerated above.
    r"\w[\w+.-]*://[^\s/@:]+:[^\s@]+@",
    r'"private_key"\s*:\s*"[^"]*"',
)

_SCRUB_FLAGS = re.IGNORECASE | re.DOTALL

# Deployment-registered extra patterns (see :func:`register_sensitive_patterns`).
_EXTRA_SENSITIVE_KEY_PATTERNS: list[str] = []
_EXTRA_LOG_STRING_PATTERNS: list[str] = []

# ....................... #


def _compile_sensitive_key_re() -> re.Pattern[str]:
    return re.compile(
        "|".join(
            (
                *_LOGFIRE_SENSITIVE_FRAGMENTS,
                *_FORZE_KEY_EXTRAS,
                *_EXTRA_SENSITIVE_KEY_PATTERNS,
            )
        ),
        _SCRUB_FLAGS,
    )


# ....................... #


def _compile_log_string_re() -> re.Pattern[str]:
    # Deliberately excludes _LOGFIRE_SENSITIVE_FRAGMENTS: those are bare key-name
    # terms for is_sensitive_key, not value patterns (see _LOG_ASSIGNMENT_FRAGMENTS).
    return re.compile(
        "|".join(
            (
                *_LOG_ASSIGNMENT_FRAGMENTS,
                *_LOG_STRING_EXTRAS,
                *_EXTRA_LOG_STRING_PATTERNS,
            )
        ),
        _SCRUB_FLAGS,
    )


# ....................... #
# Literal prefilter for scrub_log_string.
#
# The combined log-string regex (~26 alternation fragments, IGNORECASE|DOTALL)
# costs ~17 µs even on a short message that matches nothing. Most log messages
# match nothing, so we derive — from the registered patterns themselves — a set
# of lowercase literal substrings with the *superset* property: any string the
# combined regex can match must contain at least one of these literals. A plain
# substring scan over ``text.lower()`` then rejects the common no-match case in
# well under a microsecond, and only candidates fall through to the real regex.
#
# Extraction walks each fragment's parse tree (``re._parser``):
#
# * a contiguous run of required LITERAL nodes in a concatenation is a required
#   substring (the longest run is chosen);
# * an alternation is covered by the union of one required literal per branch
#   (every branch must contribute, otherwise the alternation yields nothing);
# * a required group or a repeat with ``min >= 1`` is recursed into;
# * lookarounds, anchors, character classes, and optional parts contribute
#   nothing (they are skipped — they can only make the literal set smaller).
#
# If ANY registered fragment yields no extractable required literal (e.g. a raw
# ``\d{16}`` card pattern), the prefilter is disabled outright
# (``_log_string_literals = None``) and every string goes to the regex — the
# prefilter must never be allowed to skip a string the regex would scrub.
# Non-ASCII literals also disable the prefilter: ``str.lower`` and regex
# IGNORECASE casefolding can disagree outside ASCII.


def _literals_from_nodes(nodes: Any) -> frozenset[str] | None:
    """Return lowercase literals, one of which any match of *nodes* must contain.

    *nodes* is an ``re._parser`` SubPattern (a concatenation). Returns ``None``
    when no required literal can be guaranteed.
    """

    # Longest contiguous run of required LITERAL chars in the concatenation.
    runs: list[str] = []
    current: list[str] = []

    for op, arg in nodes:
        if op is _sre_constants.LITERAL:  # type: ignore[attr-defined]
            current.append(chr(arg))  # pyright: ignore[reportArgumentType]

        else:
            if current:
                runs.append("".join(current))
                current = []

    if current:
        runs.append("".join(current))

    if runs:
        best = max(runs, key=len).lower()

        if best.isascii():
            return frozenset((best,))

        return None

    # No direct literal run: any single required composite element suffices.
    for op, arg in nodes:
        literals = _literals_from_node(op, arg)

        if literals is not None:
            return literals

    return None


# ....................... #


def _literals_from_node(op: Any, arg: Any) -> frozenset[str] | None:
    """Required-literal set for a single parse-tree node, or ``None``."""

    if op is _sre_constants.SUBPATTERN:  # type: ignore[attr-defined]
        # (group, add_flags, del_flags, subpattern) — group content is required.
        return _literals_from_nodes(arg[3])

    if op in (_sre_constants.MAX_REPEAT, _sre_constants.MIN_REPEAT):  # type: ignore[attr-defined]
        min_count, _max_count, item = arg

        if min_count >= 1:  # repeated at least once -> content is required
            return _literals_from_nodes(item)

        return None

    if op is _sre_constants.BRANCH:  # type: ignore[attr-defined]
        # (None, [alternatives]) — every alternative must contribute.
        branch_sets: list[frozenset[str]] = []

        for alternative in arg[1]:
            literals = _literals_from_nodes(alternative)

            if not literals:
                return None

            branch_sets.append(literals)

        return frozenset().union(*branch_sets)  # type: ignore[return-value]

    return None


# ....................... #


def _fragment_required_literals(fragment: str) -> frozenset[str] | None:
    """Extract required literals from one regex *fragment*; ``None`` if impossible."""

    try:
        parsed = _sre_parse.parse(fragment)  # type: ignore[attr-defined]

    except Exception:  # pragma: no cover - fragments are validated at compile
        return None

    return _literals_from_nodes(parsed)


# ....................... #


def _derive_log_string_literals() -> tuple[str, ...] | None:
    """Build the prefilter literal set; ``None`` disables the prefilter."""

    all_literals: set[str] = set()

    for fragment in (
        *_LOG_ASSIGNMENT_FRAGMENTS,
        *_LOG_STRING_EXTRAS,
        *_EXTRA_LOG_STRING_PATTERNS,
    ):
        literals = _fragment_required_literals(fragment)

        if not literals:
            return None  # safety: one opaque fragment disables the prefilter

        all_literals.update(literals)

    # Drop literals that contain another literal (the shorter one subsumes them).
    pruned = tuple(
        sorted(
            lit
            for lit in all_literals
            if not any(other != lit and other in lit for other in all_literals)
        )
    )
    return pruned


# ....................... #


@lru_cache(maxsize=4096)
def _is_sensitive_key_cached(key: str) -> bool:
    # Reads the module-global regex at call time; the cache is cleared whenever
    # the pattern set is recompiled (see _rebuild_matchers).
    return bool(_sensitive_key_re.search(key))


# ....................... #


def _rebuild_matchers() -> None:
    """Recompile matchers and regenerate all derived state.

    Single chokepoint for pattern mutation: recompiles both combined regexes,
    clears the :func:`is_sensitive_key` memo (its entries are only valid for
    one pattern set), and re-derives the :func:`scrub_log_string` prefilter
    literals. Every mutator of the pattern lists must call this.
    """

    global _sensitive_key_re, _log_string_re, _log_string_literals

    _sensitive_key_re = _compile_sensitive_key_re()
    _log_string_re = _compile_log_string_re()
    _is_sensitive_key_cached.cache_clear()
    _log_string_literals = _derive_log_string_literals()


# ....................... #

_sensitive_key_re: re.Pattern[str]
_log_string_re: re.Pattern[str]
_log_string_literals: tuple[str, ...] | None

_rebuild_matchers()

# ....................... #


def register_sensitive_patterns(
    *,
    keys: Sequence[str] = (),
    log_strings: Sequence[str] = (),
) -> None:
    """Register deployment-specific scrub patterns (case-insensitive regex fragments).

    *keys* extend the sensitive-key heuristic (:func:`is_sensitive_key`, used for both
    log-field and API-egress masking); *log_strings* extend the log-context string
    rules (:func:`scrub_log_string`). This mutates process-global scrub state and
    recompiles the matchers, so call it once during startup — before logging or serving
    begins. Empty fragments are ignored (an empty pattern would match everything).

    Registering a *log_strings* fragment with no extractable required literal
    (e.g. ``r"\\d{16}"``) disables the fast literal prefilter for
    :func:`scrub_log_string`; scrubbing stays correct but every string pays the
    full combined-regex cost.
    """

    _EXTRA_SENSITIVE_KEY_PATTERNS.extend(pattern for pattern in keys if pattern)
    _EXTRA_LOG_STRING_PATTERNS.extend(pattern for pattern in log_strings if pattern)

    _rebuild_matchers()


# ....................... #


def is_sensitive_key(key: str) -> bool:
    """Return whether *key* matches the sensitive-key heuristic.

    Memoized (bounded LRU): log/egress keys are a small repeating set, so the
    combined-regex scan runs once per distinct key per pattern-set generation.
    """

    return _is_sensitive_key_cached(key)


# ....................... #


def scrub_log_string(text: str) -> str:
    """Apply log-context string rules to *text* (Logfire-aligned patterns and extras).

    Fast path: if the derived literal prefilter is active and *text* contains
    none of the required literals, no pattern can match and *text* is returned
    untouched without running the combined regex.
    """

    literals = _log_string_literals

    if literals is not None:
        lowered = text.lower()

        for literal in literals:
            if literal in lowered:
                break

        else:
            return text

    return _log_string_re.sub(SECRET_PLACEHOLDER, text)
