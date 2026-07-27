"""Mock-coverage guard: every contract dependency key is implemented by the oracle, or triaged.

`MockDepsModule` is what unit tests and simulation resolve against, so a port it does not
register is a port whose contract cannot be exercised anywhere except an integration test
someone remembered to write. That is not hypothetical: ``GraphManagementPort`` promises
idempotent ``ensure_schema`` / ``drop_schema``, and on Neo4j provisioning is load-bearing
(without the uniqueness constraint, Cypher ``CREATE`` writes a second node under a key meant
to identify one) — yet the oracle had no implementation at all, so ``ctx.graph.management()``
raised on the one line every graph application is told to run at startup. The reference docs
even claimed "a mock implements the surface". Nobody noticed until a one-off sweep.

**A behaviour test only covers a port somebody thought to write one for; a structural check
cannot be forgotten.** A newly added ``DepKey`` either gets a mock or gets an entry here.

Each exemption names *why*. An exemption is a claim that the key is covered somewhere else,
or is not a port at all — never that the oracle "does not need" it:

- ``opt-in`` — the oracle *does* implement it, behind a flag on ``MockDepsModule``. These are
  the strongest exemptions because the guard verifies them: it enables the flag and asserts
  the key appears. A stale claim fails here.
- ``other-module`` — a different ``DepsModule`` owns and ships the implementation, so the
  mock registering it too would be wrong, not missing.
- ``config-value`` — the key carries declared configuration rather than a port, so there is
  no behaviour to stand in for.

The guard fails in **both** directions: an untriaged key is a gap, and an exemption for a key
that is now registered is a stale claim that must be deleted.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Any

import attrs

import forze.application.contracts as contracts_pkg
from forze.application.contracts.deps import DepKey
from forze.application.contracts.secrets import ExchangedCredential
from forze_mock import MockDepsModule

# ----------------------- #

_OPT_IN = "opt-in"
_OTHER_MODULE = "other-module"
_CONFIG_VALUE = "config-value"

_EXEMPTIONS: dict[str, tuple[str, str]] = {
    "hlc_checkpoint": (
        _OPT_IN,
        "MockDepsModule(hlc_checkpoint=True) wires MockHlcCheckpointAdapter; off by default "
        "so existing scenarios see no checkpoint and the outbox flush resolves none.",
    ),
    "rotating_credentials": (
        _OPT_IN,
        "MockDepsModule(rotating_credentials=<exchanger>) wires the in-memory store. The "
        "exchange is a call to the counterparty's provider, so there is no default worth "
        "guessing and the key stays unregistered until the app supplies one.",
    ),
    "authn_event_sink": (
        _OTHER_MODULE,
        "forze_identity.authn registers it per route; the contract also treats an absent "
        "sink as a no-op, so unregistered is a supported state rather than a hole.",
    ),
    "resilience_admin": (
        _OTHER_MODULE,
        "forze.application.execution.resilience.module registers the executor under it.",
    ),
    "resilience_port_policies": (
        _OTHER_MODULE,
        "forze.application.execution.resilience.module registers the per-port policy table.",
    ),
    "saga_executor": (
        _OTHER_MODULE,
        "forze.application.execution.saga.module registers the executor; resolution is "
        "guarded by deps.exists(), so an unwired saga plane is a supported state.",
    ),
    "crypto.required_reach": (
        _CONFIG_VALUE,
        "DepKey[EncryptionReach] — a declared reach value read by outbox/transport wiring, "
        "not a port with behaviour to stand in for.",
    ),
}
"""Contract keys the oracle does not register by default, each with its reason."""


# ....................... #


def _contract_dep_keys() -> dict[str, str]:
    """Every ``DepKey`` declared under ``contracts/``, mapped to where it is declared.

    Collected by import rather than by grepping for the constructor call: a key aliased or
    re-exported under a different name still resolves to the same object, and a regex over
    source would both miss those and mis-handle multi-line declarations.
    """

    found: dict[str, str] = {}

    for module_info in pkgutil.walk_packages(
        contracts_pkg.__path__,
        f"{contracts_pkg.__name__}.",
    ):
        module = importlib.import_module(module_info.name)

        for attribute in dir(module):
            value = getattr(module, attribute, None)

            if isinstance(value, DepKey):
                # setdefault: the first (deepest) declaring module wins over re-exports.
                found.setdefault(value.name, f"{module_info.name}.{attribute}")

    return found


def _registered_by(module: MockDepsModule) -> set[str]:
    """The dependency-key names *module* registers, plain and routed alike.

    Both maps matter: the identity ports are registered routed, so a plain-only view reports
    nineteen phantom gaps — which is exactly the false positive that made an earlier one-off
    sweep look 26/27 wrong and nearly buried the one real finding.
    """

    deps = module()

    return {key.name for key in deps.plain_deps} | {key.name for key in deps.routed_deps}


@attrs.define(slots=True, frozen=True)
class _NullExchanger:
    """Minimal ``CredentialExchangerPort`` — only needed to switch the opt-in on."""

    async def exchange(self, refresh_token: str) -> ExchangedCredential:
        raise NotImplementedError  # pragma: no cover — never called by this guard


# ....................... #


def test_every_contract_dep_key_is_mocked_or_triaged() -> None:
    """The ratchet: a new port either gets an oracle implementation or an exemption."""

    declared = _contract_dep_keys()
    registered = _registered_by(MockDepsModule())
    uncovered = set(declared) - registered

    untriaged = sorted(uncovered - set(_EXEMPTIONS))
    assert not untriaged, (
        "Contract dependency key(s) have no mock implementation and no exemption: "
        + ", ".join(f"{name} ({declared[name]})" for name in untriaged)
        + ". Either register a mock adapter for it in MockDepsModule, or add an entry to "
        "_EXEMPTIONS naming why the oracle does not own it."
    )

    stale = sorted(set(_EXEMPTIONS) - uncovered)
    assert not stale, (
        "Exemption(s) for key(s) the mock now registers — delete them so the list cannot "
        "rot into a place where real gaps hide: " + ", ".join(stale)
    )


def test_exemption_reasons_are_categorised() -> None:
    """Every exemption carries a known category and a non-trivial reason."""

    for name, (category, reason) in _EXEMPTIONS.items():
        assert category in {_OPT_IN, _OTHER_MODULE, _CONFIG_VALUE}, name
        assert len(reason) > 40, f"{name}: give a real reason, not a label"


def test_opt_in_exemptions_actually_register_when_enabled() -> None:
    """The ``opt-in`` claim is checked, not trusted.

    An exemption that says "the oracle does implement this, behind a flag" is only honest if
    flipping the flag produces the key. Left unverified, a renamed field or a dropped
    registration would leave the entry sitting here asserting something that is no longer
    true — an exemption is the one place a gap can hide in plain sight.
    """

    enabled: dict[str, MockDepsModule] = {
        "hlc_checkpoint": MockDepsModule(hlc_checkpoint=True),
        "rotating_credentials": MockDepsModule(rotating_credentials=_NullExchanger()),
    }

    opt_ins = {name for name, (category, _) in _EXEMPTIONS.items() if category == _OPT_IN}

    assert opt_ins == set(enabled), (
        "An opt-in exemption has no way to switch it on here; add one so the claim stays "
        f"checked: {sorted(opt_ins ^ set(enabled))}"
    )

    for name, module in enabled.items():
        assert name in _registered_by(module), (
            f"Exemption claims {name!r} is registered when its flag is on, but enabling it "
            "did not register the key."
        )


def test_the_guard_detects_a_missing_mock() -> None:
    """The guard must be able to fail — otherwise it passes by checking nothing.

    Mirrors the real ``graph_management`` gap: a contract key nothing registers and nothing
    exempts.
    """

    declared = {**_contract_dep_keys(), "newly_added_port": "contracts.newly.NewlyAddedDepKey"}
    registered = _registered_by(MockDepsModule())

    untriaged = set(declared) - registered - set(_EXEMPTIONS)

    assert untriaged == {"newly_added_port"}


def test_the_guard_detects_a_stale_exemption() -> None:
    """The reverse direction: an exemption for a key the oracle now covers is flagged."""

    exemptions: dict[str, Any] = {**_EXEMPTIONS, "document_query": (_OPT_IN, "stale entry")}
    uncovered = set(_contract_dep_keys()) - _registered_by(MockDepsModule())

    assert sorted(set(exemptions) - uncovered) == ["document_query"]
