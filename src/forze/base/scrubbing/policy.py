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

# A camelCase / PascalCase segment boundary, for the terms short enough to need anchoring.
#
# ``\b`` requires a non-word character, and ``dbPwd`` has none — so an anchored ``pwd``
# matched ``db_pwd`` but not its camelCase twin, leaving the credential unmasked under
# every naming convention that drops separators (JSON bodies, JS clients, most wire DTOs).
# Longer terms were never affected: ``api[._ -]?key`` already spans the hump because its
# separator is optional and the flags are IGNORECASE.
#
# Case-sensitive by necessity. The surrounding flags include IGNORECASE, under which
# ``[A-Z]`` also matches lowercase and the assertion would collapse into "any letter after
# any alnum" — matching exactly the mid-token runs the anchor exists to reject. ``(?-i:…)``
# scopes the flag off for the assertion alone; the term itself stays case-insensitive.
#
# Two humps, the standard pair: ``dbPwd`` (lower→upper) and ``DBPwd`` (acronym→word). The
# second requires a trailing lowercase so an all-caps run is not split mid-acronym —
# without it ``HELPWDESK`` would read as a ``pwd`` segment.
_CAMEL_BOUND = r"(?-i:(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z]))"

_SEG = rf"(?:\b|_|{_CAMEL_BOUND})"
"""Segment edge for a short sensitive term: a word boundary, an underscore, or a case hump."""

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
    rf"{_SEG}csrf{_SEG}",
    rf"{_SEG}xsrf{_SEG}",
    rf"{_SEG}jwt{_SEG}",
    rf"{_SEG}ssn{_SEG}",
)

# Extra key terms for egress/log key masking (not all appear in Logfire defaults).
# Short fragments are anchored with ``_SEG`` (the csrf/jwt convention above) so
# ``pwd`` / ``db_pwd`` / ``pwd_hash`` / ``dbPwd`` match but a mid-token run
# (``backupwd``) does not. ``uri`` is anchored at either end of a segment (``_URI``):
# unanchored it is a run inside ordinary words (``security``, ``during``,
# ``manufacturing``), while a URI name puts it first (``uri_template``) or last (``db_uri``,
# ``dbUri``, ``dburi``, ``MONGOURI``). A word that merely ends in "uri" (``Missouri``) is
# masked too: over-masking an ordinary word is the cheaper failure than leaking a
# separator-less ``dburi``.
_URI = rf"(?:{_SEG}uri|uri(?=s?(?:\d{{1,4}})?s?(?:\b|_|{_CAMEL_BOUND})))"
"""``uri`` at the start or the end of a segment, never in the middle of a word.

The end arm is a lookahead, and admits a plural and a number (either order) before the
boundary: consuming the boundary would swallow the ``_`` a compound suffix starts with
(``dburi_value=`` would leak), and without the affix ``dburis`` and ``dburi2`` would not
end a segment at all."""

_FORZE_KEY_EXTRAS: tuple[str, ...] = (
    "token",
    "dsn",
    _URI,
    "authorization",
    rf"{_SEG}pwd{_SEG}",
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
# key-only term is ``authorization``, whose *bare* value form is owned by the
# full-line ``authorization\s*[=:]`` rule below — an assignment match would stop
# at the scheme word (``Basic``) and leak the credential after it. A behavioral
# property test enforces the reconciliation: every vocabulary term, morphed
# through the naming shapes the key heuristic accepts (compounds, camelCase,
# plural, digit-suffixed), must mask in the value forms whenever
# ``is_sensitive_key`` masks it as a key.
#
# These terms are also the vocabulary of the quoted-key rule below, which is what
# catches the same credentials in a serialized body.
_LOG_ASSIGNMENT_TERM_FRAGMENTS: tuple[str, ...] = (
    "password",
    "passwd",
    r"mysql[._ -]?pwd",
    # Left-bounded like its key-heuristic twin: ``pwd=`` / ``db_pwd=`` / ``dbPwd=``
    # match, a mid-token run (``backupwd=``) stays ordinary text.
    rf"{_SEG}pwd",
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
    _URI,
)

# Quoted-key form of the same vocabulary — a credential inside a *serialized* body.
#
# The assignment rule above expects the separator to follow the name directly
# (``api_key=…`` / ``api_key: …``), but in JSON — and in a Python dict repr — the
# name's closing quote sits between them (``"api_key":"sk_live_…"``), so that rule
# never fires and a logged request/response/webhook body would go out verbatim. Any
# code path that logs a serialized payload hits this, which is most of them.
#
# The key's *closing* quote is what anchors the form (the opening one is irrelevant to a
# search), and the value is quote-delimited, so it is masked exactly — not to the next
# whitespace. That bound is also why ``authorization`` can join the vocabulary here
# (unlike the assignment rule, whose match would stop at the scheme word ``Basic`` and
# leak the credential after it): ``"authorization": "Basic dXNlcjpwYXNz"`` masks whole.
# An unquoted JSON value (a number, ``null``) is masked up to the next structural
# character, so the rest of the body survives.
_LOG_QUOTED_KEY_TERM_FRAGMENTS: tuple[str, ...] = (
    *_LOG_ASSIGNMENT_TERM_FRAGMENTS,
    "authorization",
)

_QUOTED_VALUE = r"""(?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[^\s,}\]]+)"""

# The bounded compound-name suffix between a sensitive term and its ``=``/``:``.
#
# A trailing segment is separator-led (``secret_key=``) **or** a camelCase hump
# (``secretKey=``). Only the first was recognized, so a compound name leaked its value
# under camelCase while its snake_case twin was masked — the same separator-only
# assumption ``_SEG`` fixes on the key side, one layer out.
#
# The hump alternative stays case-sensitive (``(?-i:…)``) and must begin with an uppercase
# letter, which is what keeps the deliberate non-secrets out: ``secretary=`` and
# ``tokenizer=`` continue past the term in *lowercase*, so neither alternative applies, no
# suffix is consumed, and the required separator never lines up.
#
# Atomic (``(?>…)``): the suffix is matched once, greedily, and never re-split. The
# segment alternatives overlap (``\w`` covers ``_`` and uppercase runs, which the
# separator and hump branches can also start), so with backtracking allowed a sensitive
# term followed by attacker text that never reaches ``=``/``:`` explores every way to
# partition the tail across up to six segments — polynomial blow-up, minute-scale CPU at
# a few hundred characters, on the path every logged string value takes. Atomicity is
# free here: segments consume only ``\w``/``[._-]`` characters while the required
# ``\s*[=:]`` (or closing quote) can begin with none of them, so a shorter munch can
# never turn a failed match into a successful one.
_COMPOUND_SUFFIX = r"(?>(?:[._-]\w+|(?-i:[A-Z][a-z0-9]*)){0,6})"

# Optional numbering and plural between the term and its suffix/separator:
# numbered secrets (``token2=…``, ``"api_key2": "…"``) and pluralized names
# (``tokens=…``, ``credentials: …``) carry their values too, and the key
# heuristic already treats both as sensitive — without this affix the value
# form leaked exactly the names the key path masks. Digits are bounded; the
# single optional ``s`` cannot re-admit the deliberate non-secrets
# (``secretary=`` / ``tokenizer=`` continue with other lowercase letters, so
# the required ``=``/``:`` still never lines up). The plural may come either side of
# the number (``tokens2=`` as well as ``token2s=``), as the key heuristic reads both.
_TERM_AFFIX = r"s?(?:\d{1,4})?s?"

# What follows a vocabulary term in each value form: affix, compound suffix, separator, value.
_ASSIGNMENT_TAIL = _TERM_AFFIX + _COMPOUND_SUFFIX + r"\s*[=:]\s*\S+"
_QUOTED_KEY_TAIL = _TERM_AFFIX + _COMPOUND_SUFFIX + r"[\"']\s*[=:]\s*" + _QUOTED_VALUE

_LOG_ASSIGNMENT_FRAGMENTS: tuple[str, ...] = (
    "(?:" + "|".join(_LOG_ASSIGNMENT_TERM_FRAGMENTS) + ")" + _ASSIGNMENT_TAIL,
    "(?:" + "|".join(_LOG_QUOTED_KEY_TERM_FRAGMENTS) + ")" + _QUOTED_KEY_TAIL,
)

# The email and userinfo fragments below are length-bounded AND possessive. Their
# prefilter literals (``@``, ``://``) are one character of attacker-controlled text
# away, and an *unbounded* run before a required literal costs O(run) at every start
# position the sub scan tries — quadratic over the string, ~5 s of CPU at 30 KB of
# ``aaaa…@`` — on the path every logged string takes. Bounds cap the per-position
# work at the RFC sizes real values obey (emails: 64-octet local parts, 63-octet
# labels; DSNs: schemes ≲32, userinfo far under the bounds chosen); possessiveness
# removes in-position backtracking, which is free because every bounded class
# excludes the character required next. A value exceeding a bound is out-of-spec;
# the sliding scan still masks its tail (the bound-sized window before ``@``).
_LOG_STRING_EXTRAS: tuple[str, ...] = (
    r"[a-zA-Z0-9._%+-]{1,64}+@(?:[a-zA-Z0-9-]{1,63}+\.){1,8}+[a-zA-Z]{2,63}",
    r"Bearer\s+\S+",
    # Any ``Authorization: <credential>`` value, not just the Bearer scheme; also
    # the ``=`` form (a header echoed into a query string or key=value log line)
    # and the compound/plural/numbered names the key heuristic accepts
    # (``authorization_value=…``); consume the rest of the line so ``Basic <b64>``
    # / ``Bearer <jwt>`` (scheme + credential token) are masked whole, not just
    # the scheme word. This term stays out of the assignment vocabulary — that
    # rule's ``\S+`` value would stop at the scheme word and, matching first in
    # the alternation, would preempt this rule and leak the credential after it.
    r"authorization" + _TERM_AFFIX + _COMPOUND_SUFFIX + r"\s*[=:]\s*[^\r\n]+",
    r"postgresql(?:\+[a-z]+)?://\S+",
    r"mysql(?:\+[a-z]+)?://\S+",
    r"redis(?:\+[a-z]+)?://\S+",
    r"amqps?://\S+",
    # Scheme-agnostic ``scheme://user:pass@`` userinfo, so clickhouse://, mongodb://,
    # https://user:pass@ … DSNs are masked, not only the four schemes enumerated above.
    # The user/password bounds are deliberately generous (512/4096 — far past any real
    # token-as-password), because an oversized field does not degrade to a partial
    # mask here: the scheme anchor cannot re-slide into the userinfo run, so a field
    # past its bound would leak the credential WHOLE. The bounds exist only for the
    # per-anchor DoS cap (see the block comment above); values beyond them are
    # pathological and accepted as the residual.
    r"\w[\w+.-]{0,31}+://[^\s/@:]{1,512}+:[^\s@]{1,4096}+@",
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
# (``_log_string_prefilter = None``) and every string goes to the regex — the
# prefilter must never be allowed to skip a string the regex would scrub.
# Non-ASCII literals also disable the prefilter: ``str.lower`` and regex
# IGNORECASE casefolding can disagree outside ASCII.
#
# The literals are kept per alternative, so a string that passes the prefilter
# runs only the alternatives whose literals it contains, not all ~50 of them at
# every position. The alternatives are the combined regex's top-level branches,
# with each term-vocabulary fragment distributed over its terms: ``(?:a|b)T`` and
# ``(?:a)T|(?:b)T`` try the same matches in the same order. An alternative whose
# literals are all absent can match nowhere in the string, so leaving it out
# cannot change which branch wins at any position; :func:`_sub_first_alternative`
# then replays the alternation's leftmost-first scan over the ones left.


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

    if op is _sre_constants.ATOMIC_GROUP:  # type: ignore[attr-defined]
        # ``(?>…)`` — atomicity changes cost, never what is required to match.
        return _literals_from_nodes(arg)

    if op in (
        _sre_constants.MAX_REPEAT,  # type: ignore[attr-defined]
        _sre_constants.MIN_REPEAT,  # type: ignore[attr-defined]
        _sre_constants.POSSESSIVE_REPEAT,  # type: ignore[attr-defined]
    ):
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


def _log_string_alternatives() -> tuple[str, ...]:
    """The combined log-string regex as its ordered top-level alternatives."""

    return (
        *(f"(?:{term}){_ASSIGNMENT_TAIL}" for term in _LOG_ASSIGNMENT_TERM_FRAGMENTS),
        *(f"(?:{term}){_QUOTED_KEY_TAIL}" for term in _LOG_QUOTED_KEY_TERM_FRAGMENTS),
        *_LOG_STRING_EXTRAS,
        *_EXTRA_LOG_STRING_PATTERNS,
    )


# ....................... #


_LogStringPrefilter = tuple[tuple[str, ...], dict[str, tuple[tuple[int, str], ...]]]
"""The prefilter literals, and per literal the ``(position, source)`` alternatives needing it."""


def _derive_log_string_prefilter() -> _LogStringPrefilter | None:
    """Build the prefilter; ``None`` disables it.

    Positions order the alternatives as in the combined regex. With a capturing group
    anywhere, the combined regex stays the one alternative: a backreference resolves
    against the combined numbering.
    """

    alternatives = _log_string_alternatives()
    literal_sets = [_fragment_required_literals(source) for source in alternatives]

    if not all(literal_sets):
        return None  # safety: one opaque fragment disables the prefilter

    if _log_string_re.groups:
        alternatives = (_log_string_re.pattern,)
        literal_sets = [frozenset[str]().union(*literal_sets)]  # type: ignore[arg-type]

    index: dict[str, list[tuple[int, str]]] = {}

    for position, (source, literals) in enumerate(zip(alternatives, literal_sets, strict=True)):
        for literal in literals or ():
            index.setdefault(literal, []).append((position, source))

    return tuple(index), {literal: tuple(entries) for literal, entries in index.items()}


# ....................... #


@lru_cache(maxsize=512)
def _compile_alternative(source: str) -> re.Pattern[str]:
    # Keyed by the pattern text alone (the flags are fixed), so a policy change can
    # never serve a stale pattern; compiled on first use to keep import cheap.
    return re.compile(source, _SCRUB_FLAGS)


# ....................... #

_MAX_SPLIT_ALTERNATIVES = 10
"""Above this many candidate alternatives the combined regex scans once instead."""


def _sub_first_alternative(text: str, sources: Sequence[str]) -> str:
    """``re.sub`` by the alternation of *sources* (in order), without compiling it.

    Replays the alternation's scan: the leftmost match wins, a tie goes to the earlier
    alternative, and the scan resumes where the replaced match ends. Every alternative
    requires a literal, so no match is empty and resuming needs no empty-match rule.
    """

    patterns = [_compile_alternative(source) for source in sources]

    if len(patterns) == 1:
        return patterns[0].sub(SECRET_PLACEHOLDER, text)

    pending = [pattern.search(text) for pattern in patterns]
    parts: list[str] = []
    pos = 0

    while True:
        first: re.Match[str] | None = None

        for match in pending:
            if match is not None and (first is None or match.start() < first.start()):
                first = match

        if first is None:
            break

        parts += (text[pos : first.start()], SECRET_PLACEHOLDER)
        pos = first.end()
        # A pending match starting inside the replaced span is gone; look again past it.
        pending = [
            match if match is None or match.start() >= pos else pattern.search(text, pos)
            for pattern, match in zip(patterns, pending, strict=True)
        ]

    parts.append(text[pos:])

    return "".join(parts)


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
    literals (the combined regex first: the derivation reads it). Every mutator
    of the pattern lists must call this.
    """

    global _sensitive_key_re, _log_string_re, _log_string_prefilter

    _sensitive_key_re = _compile_sensitive_key_re()
    _log_string_re = _compile_log_string_re()
    _is_sensitive_key_cached.cache_clear()
    _log_string_prefilter = _derive_log_string_prefilter()


# ....................... #

_sensitive_key_re: re.Pattern[str]
_log_string_re: re.Pattern[str]
_log_string_prefilter: _LogStringPrefilter | None

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
    full combined-regex cost. A fragment with a capturing group keeps the prefilter,
    but a string that passes it runs the full combined regex rather than only the
    patterns whose literals it contains.
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
    untouched without running the combined regex. Otherwise only the patterns
    whose literals *text* contains run, with the combined regex's result.
    """

    prefilter = _log_string_prefilter

    if prefilter is None:
        return _log_string_re.sub(SECRET_PLACEHOLDER, text)

    literals, alternatives = prefilter
    lowered = text.lower()

    if not text.isascii():
        # IGNORECASE matches ``ı`` and ``İ`` (which lowers to ``i`` + U+0307) to ``i``
        # and ``ſ`` to ``s``; fold them too, or a ``paſſword=`` value skips the regex.
        lowered = lowered.replace("\u0131", "i").replace("\u017f", "s").replace("\u0307", "")

    hits = [literal for literal in literals if literal in lowered]

    if not hits:
        return text

    entries: Sequence[tuple[int, str]] = alternatives[hits[0]]

    if len(hits) > 1:
        entries = sorted({entry for hit in hits for entry in alternatives[hit]})

    if len(entries) > _MAX_SPLIT_ALTERNATIVES:
        return _log_string_re.sub(SECRET_PLACEHOLDER, text)

    return _sub_first_alternative(text, [source for _, source in entries])
