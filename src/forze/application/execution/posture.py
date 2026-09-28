"""A deployment's production posture: rules over the application's own settings, checked while a
deployment can still be stopped.

The framework owns no settings class — the environment prefix, the nesting and the extra-key
policy are the deployment's (see :mod:`forze.base.settings`) — so the posture does not own one
either. It carries the application's settings object and declares rules over **paths** into it:
these fields must be set, these must be HTTPS, these values are development-only. The runtime
evaluates it at :func:`~forze.application.execution.check_wiring` and again when it is built, and
a violation refuses the boot naming the **keys and rules, never the values** — a boot failure is
logged, exported and pasted into a ticket, and one that echoed a DSN would put a password in all
three.

An unset environment is production. The application names the environments that are *not*
(``dev``, ``test``, ``staging``); every other value — unset, empty, ``prod``, a typo — gets every
rule, so the dangerous default is the strict one.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from enum import Enum
from ipaddress import ip_address
from typing import Any, Final, Protocol, final, runtime_checkable
from urllib.parse import SplitResult, urlsplit

import attrs
from pydantic import BaseModel, SecretBytes, SecretStr

from forze.base.exceptions import exc

# ----------------------- #

POSTURE_REFUSED: Final[str] = "production_posture_refused"
"""Code on the refusal, so a deployment can tell a posture violation from any other failure."""

_UNRESOLVED: Final = object()


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class WiringFinding:
    """A declaration-time refusal: what was checked, and which rule it broke — never its value.

    The channel every declaration-time check reports through (see
    :attr:`~forze.application.execution.WiringReport.findings`), so a finding reaches the caller
    with the rest of the wiring report instead of raising from wherever it was noticed.
    """

    source: str
    """Who reported it: ``"posture"`` for a rule, or the reporting module (``"forze_http"``)."""

    target: str
    """What it is about — a settings path, or the reporter's own name for what it checked. An
    :class:`Exempt` matches this."""

    rule: str
    """What is wrong with it, in words that carry no value."""

    def render(self) -> str:
        """``target: rule`` — the line a refusal lists."""

        return f"{self.target}: {self.rule}"


@runtime_checkable
class PostureAware(Protocol):
    """A deps module whose configs carry safety checks a production posture escalates.

    Such a check warns when it is constructed, because a config cannot know whether it is on a
    laptop or in production. The module reports the same condition here; under a production
    posture each finding refuses the boot unless the posture exempts it, and without one the
    construction-time warning is all there is.
    """

    def posture_findings(self) -> Iterable[WiringFinding]:
        """The safety findings of this module's configs, carrying no values."""
        ...  # pragma: no cover


# ....................... #


def _field_paths(value: Iterable[str], *, owner: str) -> tuple[str, ...]:
    # A bare string is an iterable of its letters: `fields="db.dsn"` would be the paths
    # "d", "b", ".", … — each unresolvable, and the rule would refuse nothing it was meant to.
    if isinstance(value, str):
        raise exc.configuration(
            f"{owner}.fields takes a collection of paths, not the string {value!r}.",
            code="production_posture_declaration",
        )

    paths = tuple(value)

    if not paths or any(not isinstance(path, str) or not path.strip() for path in paths):
        raise exc.configuration(
            f"{owner} names no field, or a blank one; a rule over nothing enforces nothing.",
            code="production_posture_declaration",
        )

    return paths


def _require_set_fields(value: Iterable[str]) -> tuple[str, ...]:
    return _field_paths(value, owner="RequireSet")


def _require_https_fields(value: Iterable[str]) -> tuple[str, ...]:
    return _field_paths(value, owner="RequireHttps")


def _loopback_fields(value: Iterable[str]) -> tuple[str, ...]:
    return _field_paths(value, owner="LoopbackHost")


def _dev_value_fields(value: Iterable[str] | None) -> tuple[str, ...] | None:
    return None if value is None else _field_paths(value, owner="DevValue")


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RequireSet:
    """Each field must be set — not ``None``, not blank, not an empty secret or collection."""

    fields: tuple[str, ...] = attrs.field(converter=_require_set_fields)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RequireHttps:
    """Each field, when set, must be an ``https`` URL; a collection, every element.

    An unset field passes: whether it must be set is :class:`RequireSet`'s question.
    """

    fields: tuple[str, ...] = attrs.field(converter=_require_https_fields)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DevValue:
    """A value containing :attr:`pattern` is development-only.

    Matched as a substring, so ``pattern="*"`` over a CORS allow-list refuses a wildcard origin,
    and ``pattern="_dev_only"`` a marker a developer put in a value to be caught. With
    :attr:`fields` unset the rule reads **every** string field of the settings, secrets
    included — which is how the shipped marker rule works without knowing any path.
    """

    pattern: str

    fields: tuple[str, ...] | None = attrs.field(default=None, converter=_dev_value_fields)

    def __attrs_post_init__(self) -> None:
        if not self.pattern:
            raise exc.configuration(
                "DevValue has an empty pattern, which every value contains.",
                code="production_posture_declaration",
            )


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LoopbackHost:
    """Each field, when set, must not point at a loopback host — a DSN or a URL whose host is
    ``localhost`` or a loopback address, or a bare ``host[:port]``."""

    fields: tuple[str, ...] = attrs.field(converter=_loopback_fields)


PostureRule = RequireSet | RequireHttps | DevValue | LoopbackHost
"""One rule of a posture."""

DEV_ONLY_MARKER: Final[DevValue] = DevValue(pattern="_dev_only")
"""A ``_dev_only`` marker anywhere in the settings is refused — the rule that needs no path."""

DEFAULT_RULES: Final[tuple[PostureRule, ...]] = (DEV_ONLY_MARKER,)
"""What a posture checks when it declares nothing: only the marker, since every other rule needs
the application's own paths. Extend it rather than replace it: ``rules=(*DEFAULT_RULES, ...)``."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Exempt:
    """A target a production posture does not refuse, with the reason it does not.

    The reason is required and cannot be blank, so an exemption reads as a reviewed line in the
    deployment's own declaration — the service mesh that terminates TLS in a sidecar — rather
    than as a switch someone flipped.
    """

    target: str
    """A settings path, or a reported finding's target."""

    reason: str

    def __attrs_post_init__(self) -> None:
        if not self.target.strip() or not self.reason.strip():
            raise exc.configuration(
                "An Exempt needs a target and a non-blank reason; an exemption nobody explained "
                "is one nobody reviewed.",
                code="production_posture_declaration",
            )


# ....................... #


def _environments(value: Iterable[str]) -> frozenset[str]:
    if isinstance(value, str):
        raise exc.configuration(
            f"ProductionPosture.non_production takes a collection of names, not {value!r}: a "
            "bare string is a set of its letters, and every real environment name would read "
            "as production.",
            code="production_posture_declaration",
        )

    return frozenset(value)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ProductionPosture:
    """What a production deployment of this application must not be started without.

    Evaluated at :func:`~forze.application.execution.check_wiring` (``posture=``) and when the
    :class:`~forze.application.execution.ExecutionRuntime` is built — the first the early signal
    for an application that runs the check, the second the floor for one that does not.
    """

    settings: BaseModel
    """The application's settings object, which every rule reads by path."""

    environment: str | None = None
    """Path to the field naming the environment; ``None`` means every deployment is production."""

    non_production: frozenset[str] = attrs.field(factory=frozenset, converter=_environments)
    """Environment values under which the value rules are inert — the application's own names.
    Any other value, including unset and empty, is production."""

    rules: tuple[PostureRule, ...] = DEFAULT_RULES
    """The rules checked in production."""

    exempt: tuple[Exempt, ...] = ()
    """Targets not refused, each with its reason."""

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.settings, BaseModel):
            raise exc.configuration(
                "ProductionPosture.settings must be the application's settings model (a "
                f"pydantic BaseModel), got {type(self.settings).__name__}.",
                code="production_posture_declaration",
            )

    # ....................... #

    @property
    def is_production(self) -> bool:
        """Whether the value rules apply: the environment is unset, empty, or not declared
        non-production. An unresolvable environment path reads as production too."""

        if self.environment is None:
            return True

        # Unset or unresolvable yields no name, and `all` of nothing is production.
        return all(
            name.strip() not in self.non_production
            for name in _texts(_resolve(self.settings, self.environment))
        )

    # ....................... #

    def findings(self, reported: Iterable[WiringFinding] = ()) -> tuple[WiringFinding, ...]:
        """Every refusal this posture makes of its settings and of *reported* module findings.

        A path that does not resolve is reported in every environment — it is a defect in the
        declaration, which a laptop should find. The value rules and the reported findings
        apply only in production, and an exempted target is never refused.
        """

        # A declaration error cannot be exempted: an exemption names a target the posture
        # refuses, and a path that resolves to nothing is not being checked at all.
        declared = [
            WiringFinding(
                source="posture",
                target=path,
                rule=f"does not resolve in {type(self.settings).__name__}",
            )
            for path in self._declared_paths()
            if _resolve(self.settings, path) is _UNRESOLVED
        ]
        refused: list[WiringFinding] = []

        if self.is_production:
            exempted = {entry.target for entry in self.exempt}
            candidates = [finding for rule in self.rules for finding in self._check(rule)]
            refused = [
                finding for finding in (*candidates, *reported) if finding.target not in exempted
            ]

        return tuple({(f.target, f.rule): f for f in (*declared, *refused)}.values())

    # ....................... #

    def _declared_paths(self) -> Iterator[str]:
        if self.environment is not None:
            yield self.environment

        for rule in self.rules:
            yield from rule.fields or ()

    def _check(self, rule: PostureRule) -> Iterator[WiringFinding]:
        match rule:
            case RequireSet(fields=fields):
                for path in fields:
                    value = _resolve(self.settings, path)

                    if value is not _UNRESOLVED and not _is_set(value):
                        yield WiringFinding(
                            source="posture", target=path, rule="required and unset"
                        )

            case RequireHttps(fields=fields):
                for path in fields:
                    if any(
                        (split := _split(text)) is None
                        or split.scheme.lower() != "https"
                        or not split.hostname
                        for text in _texts(_resolve(self.settings, path))
                    ):
                        yield WiringFinding(source="posture", target=path, rule="must be https")

            case LoopbackHost(fields=fields):
                for path in fields:
                    if any(_is_loopback(text) for text in _texts(_resolve(self.settings, path))):
                        yield WiringFinding(
                            source="posture", target=path, rule="points at a loopback host"
                        )

            case DevValue(pattern=pattern, fields=fields):
                pairs = (
                    ((path, _resolve(self.settings, path)) for path in fields)
                    if fields is not None
                    else _leaves(self.settings, "")
                )

                for path, value in pairs:
                    if any(pattern in text for text in _texts(value)):
                        yield WiringFinding(
                            source="posture",
                            target=path,
                            rule=f"development-only value (contains {pattern!r})",
                        )

    # ....................... #

    def enforce(self, reported: Iterable[WiringFinding] = ()) -> None:
        """Refuse the deployment if :meth:`findings` finds anything.

        :raises CoreException: ``configuration``, code ``production_posture_refused``, listing
            each finding's target and rule — never a value.
        """

        refuse_findings(self.findings(reported))


def refuse_findings(findings: Sequence[WiringFinding]) -> None:
    """Raise one refusal listing *findings*, or return when there are none."""

    if not findings:
        return

    lines = "\n".join(f"  - {finding.render()}" for finding in findings)

    raise exc.configuration(
        f"The production posture refuses this deployment ({len(findings)} finding(s)):\n{lines}\n"
        "Fix the settings, or declare the environment non-production. Values are never shown; "
        "each line names the setting and the rule it breaks.",
        code=POSTURE_REFUSED,
        details={
            "findings": [{"source": f.source, "target": f.target, "rule": f.rule} for f in findings]
        },
    )


# ....................... #


def _resolve(root: object, path: str) -> Any:
    """The value at dotted *path* under *root*, ``None`` through an unset parent, or
    :data:`_UNRESOLVED` when a segment names nothing the settings declare."""

    node: Any = root

    for segment in path.split("."):
        if node is None:
            return None

        if isinstance(node, BaseModel):
            node = _fields(node)

        if isinstance(node, Mapping):
            if segment not in node:
                return _UNRESOLVED

            node = node[segment]

        else:
            return _UNRESOLVED

    return node


def _leaves(node: object, path: str) -> Iterator[tuple[str, Any]]:
    """Every field under *node* with its path — models and mappings walked, anything else a leaf."""

    if isinstance(node, BaseModel):
        node = _fields(node)

    if isinstance(node, Mapping):
        for key, value in node.items():
            yield from _leaves(value, f"{path}.{key}" if path else str(key))

    else:
        yield path, node


def _texts(value: Any) -> Iterator[str]:
    """The strings a value holds — a secret's own text, a URL's rendering, each element of a
    collection — and nothing for an unset or unresolved one."""

    if value is None or value is _UNRESOLVED:
        return

    if isinstance(value, SecretStr):
        yield value.get_secret_value()

    elif isinstance(value, SecretBytes):
        yield value.get_secret_value().decode("utf-8", errors="replace")

    elif isinstance(value, Enum):
        yield str(value.value)

    elif isinstance(value, str):
        yield value

    elif isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            yield from _texts(item)

    elif isinstance(value, (BaseModel, Mapping)):
        # A group: every value under it, so a rule or the scan never passes one by not looking.
        for _, leaf in _leaves(value, ""):
            yield from _texts(leaf)

    else:
        yield str(value)


def _fields(model: BaseModel) -> dict[str, Any]:
    """A model's fields by name, keys kept by ``extra="allow"`` included."""

    return {name: getattr(model, name) for name in type(model).model_fields} | (
        model.model_extra or {}
    )


def _split(text: str) -> SplitResult | None:
    # The parser refuses a netloc with an unmatched or non-address bracket — an unencoded
    # password can hold one — and its error may quote part of it. Unparseable is an answer.
    try:
        return urlsplit(text)

    except ValueError:
        return None


def _is_set(value: Any) -> bool:
    if value is None:
        return False

    if isinstance(value, (SecretStr, SecretBytes)):
        return bool(value.get_secret_value().strip())

    if isinstance(value, (str, bytes)):
        return bool(value.strip())

    if isinstance(value, (list, tuple, set, frozenset, Mapping)):
        return bool(value)

    return True


def _is_loopback(text: str) -> bool:
    split = _split(text if "//" in text else f"//{text}")

    # A multi-host DSN (`postgresql://a:5432,b:5432/db`) lists hosts the parser reads as one.
    hosts = split.netloc.rpartition("@")[2].split(",") if split is not None else []

    return any(_is_loopback_host(host) for host in hosts)


def _is_loopback_host(entry: str) -> bool:
    split = _split(f"//{entry}")
    host = split.hostname if split is not None else None

    if not host:
        return False

    if host == "localhost" or host.endswith(".localhost"):
        return True

    try:
        return ip_address(host).is_loopback

    except ValueError:
        return False
