"""Tests for the conformance ratchet (.github/scripts/conformance_manifest.py).

The checker exists because "a differential leg exists for this plane" was a fact recorded
in prose, so a plane could gain a backend or lose a leg with CI staying green. These tests
feed it synthetic manifests and censuses and pin the gate semantics — above all that it
**can fail**, in each of the ways it is supposed to:

- a contract key nobody claimed, a claim on a key that no longer exists, a key claimed twice;
- a manifested leg pytest cannot collect, and a collected leg the manifest never declared;
- a scenario reference that no longer resolves, and a divergence probe pointing at a dead test;
- an exemption whose claim about the world has gone stale;
- and the ratchet proper: a backend that registers a plane's ports without running its leg.

The repo's own manifest is then validated for shape, so the table cannot rot into a place
where a gap hides behind a missing reason.
"""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / ".github" / "scripts" / "conformance_manifest.py"


def _load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("conformance_manifest", _SCRIPT)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load checker script at {_SCRIPT}")

    module = importlib.util.module_from_spec(spec)
    # dataclass processing resolves the defining module via sys.modules, so the script must
    # be registered before exec, like a normal import would.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


checker = _load_checker()


# ....................... #


def _plane(
    *,
    name: str = "counter",
    scenario: str = "forze_dst.conformance.counters:run_counter_allocation",
    dep_keys: tuple[str, ...] = ("counter",),
    engines: tuple[str, ...] = ("mock", "postgres"),
    waivers: dict[str, str] | None = None,
):
    return checker.Plane(
        name=name,
        scenario=scenario,
        dep_keys=dep_keys,
        engines=engines,
        waivers=waivers or {},
    )


def _manifest(
    *,
    planes=(),
    gaps=(),
    exemptions=None,
    catalog: str | None = None,
):
    return checker.Manifest(
        planes={plane.name: plane for plane in planes},
        gaps={gap.name: gap for gap in gaps},
        exemptions=exemptions or {},
        engine_packages={"mock": "forze_mock"},
        divergence_catalog=catalog,
    )


def _census(*, legs=None, node_ids=(), malformed=()):
    return checker.Census(
        legs=legs or {},
        node_ids=frozenset(node_ids),
        malformed=tuple(malformed),
    )


def _exemption(kind: str, reason: str = "x" * 60):
    return checker.Exemption(kind=kind, reason=reason)


def _violations(report) -> str:
    return "\n".join(report.violations)


# ----------------------- #
# Key triage


def test_an_untriaged_contract_key_fails() -> None:
    """A new port that nobody claimed is the case the whole table exists to catch."""

    report = checker.Report()
    checker.check_key_triage(
        _manifest(planes=[_plane()]),
        {"counter": "contracts.counter", "newly_added": "contracts.newly"},
        report,
    )

    assert "newly_added" in _violations(report)
    assert "counter" not in _violations(report).replace("newly_added", "")


def test_a_claim_on_a_deleted_key_fails() -> None:
    """The table must not rot: an entry for a port that is gone is a stale claim."""

    report = checker.Report()
    checker.check_key_triage(
        _manifest(planes=[_plane(dep_keys=("counter", "removed_port"))]),
        {"counter": "contracts.counter"},
        report,
    )

    assert "removed_port" in _violations(report)
    assert "stale" in _violations(report)


def test_a_key_claimed_twice_fails() -> None:
    """One owner per key, or the table says two things about the same port."""

    report = checker.Report()
    checker.check_key_triage(
        _manifest(
            planes=[_plane()],
            exemptions={"counter": _exemption(checker._SINGLE_ENGINE)},
        ),
        {"counter": "contracts.counter"},
        report,
    )

    assert "claimed more than once" in _violations(report)


# ----------------------- #
# Legs


def test_a_manifested_leg_that_is_not_collected_fails() -> None:
    """A leg deleted or renamed by a refactor must not pass as still present."""

    report = checker.Report()
    checker.check_planes(
        _manifest(planes=[_plane()]),
        {},
        _census(legs={("counter", "mock"): ("tests/x.py::test_y",)}),
        report,
    )

    assert "declares engine 'postgres'" in _violations(report)


def test_a_collected_leg_the_manifest_never_declared_fails() -> None:
    """The reverse direction: a marker naming a plane or engine nothing requires."""

    report = checker.Report()
    manifest = _manifest(planes=[_plane()])

    checker.check_census_is_declared(
        manifest,
        _census(legs={("counter", "cassandra"): ("tests/x.py::test_y",)}),
        report,
    )
    checker.check_census_is_declared(
        manifest,
        _census(legs={("ledger", "postgres"): ("tests/x.py::test_z",)}),
        report,
    )

    assert "marked as engine 'cassandra'" in _violations(report)
    assert "does not declare" in _violations(report)


def test_a_marker_missing_its_keywords_fails() -> None:
    report = checker.Report()
    checker.check_census_is_declared(
        _manifest(planes=[_plane()]),
        _census(malformed=("tests/x.py::test_y",)),
        report,
    )

    assert "needs both plane= and engine=" in _violations(report)


def test_a_scenario_that_no_longer_resolves_fails() -> None:
    report = checker.Report()
    checker.check_planes(
        _manifest(planes=[_plane(scenario="forze_dst.conformance.counters:gone_away")]),
        {},
        _census(legs={("counter", "mock"): (), ("counter", "postgres"): ()}),
        report,
    )

    assert "does not resolve" in _violations(report)


# ----------------------- #
# The ratchet


def test_a_backend_registering_a_plane_port_without_a_leg_fails() -> None:
    """The headline: a new forze_<name> cannot merge as a 'the route works' claim.

    This is the exact event that went unnoticed before — a plane gained a backend whose
    only test ran against that backend alone, with nothing comparing it to anything.
    """

    report = checker.Report()
    checker.check_planes(
        _manifest(planes=[_plane()]),
        {"counter": {"forze_postgres", "forze_cassandra"}},
        _census(legs={("counter", "mock"): (), ("counter", "postgres"): ()}),
        report,
    )

    assert "cassandra" in _violations(report)
    assert "run no conformance leg" in _violations(report)


def test_a_waiver_lets_a_registered_backend_through_but_only_with_a_reason() -> None:
    report = checker.Report()
    checker.check_planes(
        _manifest(
            planes=[
                _plane(waivers={"cassandra": "y" * 60}),
                _plane(name="ledger", dep_keys=("ledger",), waivers={"riak": "too short"}),
            ]
        ),
        {"counter": {"forze_postgres", "forze_cassandra"}, "ledger": {"forze_riak"}},
        _census(
            legs={
                ("counter", "mock"): (),
                ("counter", "postgres"): (),
                ("ledger", "mock"): (),
                ("ledger", "postgres"): (),
            }
        ),
        report,
    )

    assert "cassandra" not in _violations(report)
    assert "waiver for 'riak' needs a real reason" in _violations(report)


def test_a_waiver_for_a_backend_that_no_longer_registers_the_port_is_stale() -> None:
    report = checker.Report()
    checker.check_planes(
        _manifest(planes=[_plane(waivers={"cassandra": "y" * 60})]),
        {"counter": {"forze_postgres"}},
        _census(legs={("counter", "mock"): (), ("counter", "postgres"): ()}),
        report,
    )

    assert "waiver for 'cassandra' is stale" in _violations(report)


# ----------------------- #
# Exemptions


def test_a_single_engine_exemption_fails_once_a_second_backend_lands() -> None:
    """The claim is checked, not trusted — this is when 'nothing to compare' stops being true."""

    report = checker.Report()
    checker.check_exemptions(
        _manifest(exemptions={"cache": _exemption(checker._SINGLE_ENGINE)}),
        {"cache": {"forze_redis", "forze_memcached"}},
        report,
    )

    assert "claims a single engine" in _violations(report)
    assert "forze_memcached" in _violations(report)


def test_a_no_engine_matrix_exemption_fails_once_any_backend_registers_it() -> None:
    report = checker.Report()
    checker.check_exemptions(
        _manifest(exemptions={"authz_scope": _exemption(checker._NO_ENGINE_MATRIX)}),
        {"authz_scope": {"forze_postgres"}},
        report,
    )

    assert "the claim is stale" in _violations(report)


def test_an_exemption_needs_a_known_kind_and_a_real_reason() -> None:
    report = checker.Report()
    checker.check_exemptions(
        _manifest(
            exemptions={
                "a": _exemption("because-i-said-so"),
                "b": _exemption(checker._CONFIG_VALUE, reason="n/a"),
            }
        ),
        {},
        report,
    )

    assert "unknown kind" in _violations(report)
    assert "not a label" in _violations(report)


def test_a_gap_must_name_every_backend_it_leaves_uncovered() -> None:
    """A gap that understates its blast radius reads as smaller than it is."""

    report = checker.Report()
    checker.check_gaps(
        _manifest(
            gaps=[
                checker.Gap(
                    name="queue",
                    dep_keys=("queue_command",),
                    engines=("rabbitmq",),
                    reason="z" * 60,
                )
            ]
        ),
        {"queue_command": {"forze_rabbitmq", "forze_sqs"}},
        report,
    )

    assert "does not list engine(s) sqs" in _violations(report)


# ----------------------- #
# Divergence probes


def test_a_divergence_probe_pointing_at_a_dead_test_fails() -> None:
    """A catalogued divergence whose probe was deleted is folklore again."""

    report = checker.Report()
    checker.check_divergence_probes(
        _manifest(
            planes=[_plane()],
            catalog="forze_dst.conformance.catalog:PLANE_DIVERGENCES",
        ),
        _census(node_ids=()),
        report,
    )

    assert "which pytest does not collect" in _violations(report)


def test_the_repo_catalog_probes_are_all_resolvable_in_principle() -> None:
    """Every row names a probe and a plane the manifest declares (link liveness is CI's job)."""

    from forze_dst.conformance.catalog import PLANE_DIVERGENCES

    manifest = checker.load_manifest(_REPO / "pyproject.toml")

    for plane, rows in PLANE_DIVERGENCES.items():
        assert plane in manifest.planes, f"catalog plane {plane!r} is not manifested"

        for row in rows:
            assert row.plane == plane, f"{row.name}: row is filed under the wrong plane"
            assert row.probe.startswith("tests/"), f"{row.name}: probe is not a test node id"
            assert len(row.reason) > 80, f"{row.name}: give a real reason"


# ----------------------- #
# The repo's own table


def test_the_repo_manifest_has_the_shape_the_checker_expects() -> None:
    manifest = checker.load_manifest(_REPO / "pyproject.toml")

    assert manifest.planes, "the manifest declares no planes at all"

    for plane in manifest.planes.values():
        assert ":" in plane.scenario, f"{plane.name}: scenario must be module:attribute"
        assert plane.engines, f"{plane.name}: a plane with no engines gates nothing"

    for gap in manifest.gaps.values():
        assert len(gap.reason) >= checker._MIN_REASON_LENGTH, gap.name

    for name, exemption in manifest.exemptions.items():
        assert exemption.kind in checker._EXEMPTION_KINDS, name
        assert len(exemption.reason) >= checker._MIN_REASON_LENGTH, name


def test_every_contract_key_is_claimed_exactly_once_today() -> None:
    """The live triage, run as a unit test so a new DepKey fails fast, not only in CI."""

    manifest = checker.load_manifest(_REPO / "pyproject.toml")
    report = checker.Report()

    checker.check_key_triage(manifest, checker.contract_dep_keys(), report)

    assert not report.violations, _violations(report)


def test_the_provider_census_reads_registrations_not_reads() -> None:
    """The census must distinguish registering a port from resolving one.

    Every field-encrypting adapter resolves the keyring key; if that counted as providing
    it, fifteen packages would look like keyring backends and the ratchet would drown in
    waivers. The storage keys are the opposite trap: s3 and gcs register nothing of their
    own, inheriting the bindings from a shared deps base, so a census that only read each
    package's own files would report the plane as having no providers at all.
    """

    providers = checker.provider_census(_REPO / "src")

    assert providers.get("crypto.keyring", set()) == set(), "consumers counted as providers"
    assert providers["storage_query"] >= {"forze_s3", "forze_gcs"}, "inherited bindings missed"
    assert providers["counter"] >= {"forze_postgres", "forze_redis", "forze_mongo"}


def test_the_marker_is_registered_so_strict_markers_accepts_it() -> None:
    with (_REPO / "pyproject.toml").open("rb") as fh:
        config = tomllib.load(fh)

    markers = config["tool"]["pytest"]["ini_options"]["markers"]

    assert any(marker.startswith("conformance(") for marker in markers)


@pytest.mark.parametrize(
    "reference",
    [
        "forze_dst.conformance.counters:run_counter_allocation",
        "forze_dst.conformance.catalog:PLANE_DIVERGENCES",
    ],
)
def test_scenario_references_resolve(reference: str) -> None:
    assert checker.resolve_scenario(reference) is not None
