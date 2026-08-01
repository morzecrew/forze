"""The per-plane divergence catalog — every known difference, with the test that pins it.

:mod:`~forze_dst.conformance.divergence` catalogs the isolation family, where a divergence
is a *mechanism* difference the differential must normalise away. This module catalogs the
other planes, where the question is different: what did two implementations of one port
actually disagree about, and what was done about it.

Each row carries a ``probe`` naming the test that asserts the row is still true, and
``.github/scripts/conformance_manifest.py`` resolves those links against real pytest
collection. That is the whole point of writing this down as data. A catalog of prose
decays into folklore — someone deletes the test, the paragraph survives, and the repo goes
on claiming a guarantee nothing checks. A catalog whose links are verified cannot.

Read a row's :class:`DivergenceResolution` as the claim it makes:

- ``UNIFIED`` — the difference is gone. The adapters were changed so every engine now does
  the same thing, and the probe fails if one drifts back.
- ``NORMALIZED`` — the difference is real and both engines are correct. The differential
  compares past it deliberately, and the probe pins the comparison it *does* make.
- ``DECLARED`` — the difference is real, unresolved, and visible in the contract. This is
  the honest kind: it says a caller cannot write one branch for every backend here.
"""

from __future__ import annotations

from enum import StrEnum

import attrs

from forze.base.exceptions import exc

# ----------------------- #


class DivergenceResolution(StrEnum):
    """What was done about a divergence once it was found."""

    UNIFIED = "unified"
    NORMALIZED = "normalized"
    DECLARED = "declared"


# ....................... #


@attrs.frozen(kw_only=True)
class EngineBehaviour:
    """What one engine did, in its own terms."""

    engine: str
    behaviour: str


# ....................... #


@attrs.frozen(kw_only=True)
class PlaneDivergence:
    """One reviewed difference between implementations of a plane's port."""

    plane: str
    """The conformance plane this belongs to; must be a plane the manifest declares."""

    name: str
    """A short stable handle, used in failure messages and cross-references."""

    observed: tuple[EngineBehaviour, ...]
    """What each engine did when the divergence was found. At least two, or it is not one."""

    resolution: DivergenceResolution
    reason: str
    """Why the resolution is the right one — the part a reviewer has to agree with."""

    probe: str
    """The pytest node id that asserts this row. Checked against collection, not trusted."""

    def __attrs_post_init__(self) -> None:
        if len(self.observed) < 2:
            raise exc.configuration(
                "a divergence needs at least two engines to diverge between",
                details={"plane": self.plane, "name": self.name},
            )

        if not self.probe:
            raise exc.configuration(
                "a catalogued divergence must name the probe that asserts it",
                details={"plane": self.plane, "name": self.name},
            )


# ----------------------- #


COUNTER_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="counter",
        name="value-domain-int64",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "counted with unbounded Python integers: incr past 2**63, decr past "
                    "-2**63 and reset(2**70) all succeeded"
                ),
            ),
            EngineBehaviour(
                engine="postgres",
                behaviour="bigint column; the store refuses anything outside int64",
            ),
            EngineBehaviour(engine="redis", behaviour="64-bit signed integer counters"),
            EngineBehaviour(engine="mongo", behaviour="NumberLong; $inc refuses to overflow it"),
            EngineBehaviour(engine="firestore", behaviour="int64 field; the client refuses more"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "The oracle was strictly more permissive than every system it stands in for, "
            "which is the mock-horizon violation in its purest form: code written against it "
            "was correct right up until production. The contract now declares the domain "
            "(COUNTER_MIN_VALUE/COUNTER_MAX_VALUE, the intersection of what the real stores "
            "hold) and the mock enforces it on every verb, so the oracle can no longer "
            "certify an allocation no backend can keep."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_counter_conformance.py::"
            "test_counter_battery[check_the_value_domain_is_int64]"
        ),
    ),
    PlaneDivergence(
        plane="counter",
        name="ceiling-crossing-refusal-kind",
        observed=(
            EngineBehaviour(
                engine="postgres",
                behaviour="raises precondition (core.precondition), value unchanged",
            ),
            EngineBehaviour(
                engine="firestore",
                behaviour=(
                    "raises precondition (counter_value_out_of_range) — its arithmetic runs "
                    "in Python, so the adapter knows the result before writing it"
                ),
            ),
            EngineBehaviour(
                engine="redis",
                behaviour=(
                    "raises infrastructure (core.infrastructure) carrying the server's "
                    "'increment or decrement would overflow', value unchanged"
                ),
            ),
            EngineBehaviour(
                engine="mongo",
                behaviour=(
                    "raises infrastructure (core.infrastructure) carrying BadValue 'Failed to "
                    "apply $inc operations to current value', value unchanged"
                ),
            ),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "Measured against live servers, not assumed: no engine wraps and no engine "
            "stores an out-of-domain value — every one refuses and leaves the counter "
            "exactly where it was, which is what the battery asserts. What they disagree "
            "about is the KIND, and that disagreement has teeth: the egress policy treats "
            "infrastructure as retryable, so on Redis and Mongo a permanently impossible "
            "allocation is reported as something worth retrying forever. Unifying it would "
            "mean recognising each store's overflow from its error text, since for three of "
            "the four engines the arithmetic happens inside the store and the adapter cannot "
            "know the result before asking. That trade — fragile text matching for a uniform "
            "kind — was rejected deliberately, so the divergence is declared here instead of "
            "being quietly smoothed over."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_counter_conformance.py::"
            "test_counter_battery[check_crossing_the_ceiling_is_refused_whole]"
        ),
    ),
    PlaneDivergence(
        plane="counter",
        name="reset-bounds-checked-before-storing",
        observed=(
            EngineBehaviour(
                engine="redis",
                behaviour=(
                    "GETSET is not bounds-checked, so reset(2**70) SUCCEEDED and the counter "
                    "broke on the next allocation — a different call, for a different caller"
                ),
            ),
            EngineBehaviour(
                engine="postgres",
                behaviour="the bigint column refused the write at the moment it was made",
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "A reset's value is known up front, so there is no reason for any backend to "
            "discover the problem later. Every adapter now validates before storing and "
            "raises the shared counter_value_out_of_range. The Redis case is the one worth "
            "remembering: a refusal that arrives late, elsewhere, and blamed on the wrong "
            "caller is worse than no refusal at all."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_counter_conformance.py::"
            "test_counter_battery[check_a_reset_outside_the_domain_is_refused_before_it_is_stored]"
        ),
    ),
    PlaneDivergence(
        plane="counter",
        name="tenant-and-route-live-in-the-key",
        observed=(
            EngineBehaviour(
                engine="firestore",
                behaviour="the only engine with a test proving two tenants keep separate sequences",
            ),
            EngineBehaviour(
                engine="mock",
                behaviour="partitions per tenant in its own store, so it cannot demonstrate the rule",
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Tenant and route belong in the counter's key, and until now exactly one backend "
            "asserted it while the oracle got the answer right for a reason that does not "
            "generalise — it hard-partitions its store per tenant, so a shared-store leak is "
            "unrepresentable there. The shared battery now drives two tenants across two "
            "suffixes with a different number of allocations each, so a key that drops the "
            "tenant, drops the suffix, or drops both lands on three distinguishable wrong "
            "answers instead of one vague inequality."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_counter_conformance.py::"
            "test_counter_battery[check_tenants_and_suffixes_are_four_disjoint_sequences]"
        ),
    ),
)


# ....................... #


STORAGE_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="storage",
        name="delete-of-a-missing-key",
        observed=(
            EngineBehaviour(engine="s3", behaviour="returns 204 — deleting nothing is a success"),
            EngineBehaviour(engine="gcs", behaviour="returns 404"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Delete is a postcondition: the caller wants the key gone, and it is. The "
            "adapter swallows not-found so retrying a delete after a timeout does not turn a "
            "success into an error, and the port documents the idempotence."
        ),
        probe=(
            "tests/integration/test_forze_gcs/test_gcs_storage_conformance.py::"
            "test_storage_battery[check_deleting_a_missing_object_is_a_no_op]"
        ),
    ),
    PlaneDivergence(
        plane="storage",
        name="second-abort-of-a-multipart-upload",
        observed=(
            EngineBehaviour(engine="s3", behaviour="MinIO tolerated it; floci raised NoSuchUpload"),
            EngineBehaviour(engine="mock", behaviour="tolerated it"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "The port already promised that aborting an already-aborted or never-started "
            "session does not error — a documented guarantee nothing implemented on one of "
            "two S3 servers. Found only because the S3 suite runs its matrix over two "
            "independent implementations; a single-server suite would have called the plane "
            "consistent. Fixing it also required correcting NoSuchUpload's mapping from "
            "infrastructure to not-found: the first fix did not fire because the error was "
            "classified as retryable."
        ),
        probe=(
            "tests/integration/test_forze_s3/test_s3_storage_conformance.py::"
            "test_storage_battery[floci-check_aborting_an_upload_twice_is_a_no_op]"
        ),
    ),
    PlaneDivergence(
        plane="storage",
        name="copy-onto-the-same-key",
        observed=(
            EngineBehaviour(engine="s3", behaviour="MinIO refuses (as does AWS S3); floci allows"),
            EngineBehaviour(engine="gcs", behaviour="allows"),
            EngineBehaviour(engine="mock", behaviour="allows"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Three of four allowed it and the reference implementation refuses, so 'allowed' "
            "was never portable. Both adapters now refuse uniformly, per the plane's own "
            "doctrine: a divergence between backends is a finding to fix in the adapter or "
            "to declare, never to special-case per backend."
        ),
        probe=(
            "tests/integration/test_forze_s3/test_s3_storage_conformance.py::"
            "test_storage_battery[minio-check_copying_onto_the_same_key_is_refused]"
        ),
    ),
    PlaneDivergence(
        plane="storage",
        name="list-ordering",
        observed=(
            EngineBehaviour(engine="mock", behaviour="insertion order"),
            EngineBehaviour(engine="s3", behaviour="lexicographic by key"),
            EngineBehaviour(engine="gcs", behaviour="lexicographic by key"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Masked for a long time by uuid7 keys, which are time-sortable, so generated keys "
            "agreed by accident and only caller-supplied keys could expose it. The mock now "
            "orders lexicographically and the port says so."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_storage_conformance.py::"
            "test_storage_battery[check_listing_is_ordered_by_key]"
        ),
    ),
)


# ....................... #


INFERENCE_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="inference",
        name="oversized-stream-chunk",
        observed=(
            EngineBehaviour(engine="mock", behaviour="refused the chunk"),
            EngineBehaviour(engine="kserve_v2", behaviour="sub-batched it to the cap and served"),
            EngineBehaviour(engine="mlflow", behaviour="sub-batched it to the cap and served"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "The oracle was stricter than reality, so correct streaming code failed only "
            "against the oracle. The root cause was a documentation one: the capability said "
            "only that an oversized predict_many is refused, and the mock author read that as "
            "blanket. Both halves are now binding, and the mock mirrors the sub-batching."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_inference_conformance.py::"
            "test_inference_battery[check_a_stream_chunk_over_the_cap_is_served_not_refused]"
        ),
    ),
    PlaneDivergence(
        plane="inference",
        name="already-spent-budget",
        observed=(
            EngineBehaviour(engine="mock", behaviour="served a prediction"),
            EngineBehaviour(engine="local", behaviour="raised cpu_offload_deadline"),
            EngineBehaviour(engine="kserve_v2", behaviour="raised inference_timeout mid-call"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "A spent budget was unobservable anywhere but a live endpoint, and the two real "
            "implementations disagreed about which failure it was. Pre-flight versus mid-call "
            "is a materially different fact on a paid endpoint — 'was I billed, did anything "
            "run' is only answerable if the two are distinguishable — so all four now refuse "
            "before the backend with one shared code."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_inference_conformance.py::"
            "test_inference_battery[check_an_exhausted_budget_refuses_before_the_backend]"
        ),
    ),
    PlaneDivergence(
        plane="inference",
        name="bare-scalar-prediction",
        observed=(
            EngineBehaviour(engine="mlflow", behaviour="accepted a bare scalar output"),
            EngineBehaviour(engine="mock", behaviour="rejected it"),
            EngineBehaviour(engine="local", behaviour="rejected it"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "A bare scalar is sklearn's own predict shape, and both docs pages already "
            "promised plane-wide wrapping — yet the most natural local model failed at the "
            "port boundary, and local is production code, not a test double. The rule moved "
            "into shape_outputs so it has one implementation instead of two."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_inference_conformance.py::"
            "test_a_scalar_returning_stub_wraps_into_a_single_field_output"
        ),
    ),
)


# ....................... #


SEARCH_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="search",
        name="blank-query-semantics",
        observed=(
            EngineBehaviour(engine="postgres", behaviour="a blank query matches everything"),
            EngineBehaviour(engine="mongo", behaviour="a blank query matches everything"),
            EngineBehaviour(engine="meilisearch", behaviour="a blank query matches everything"),
            EngineBehaviour(engine="mock", behaviour="a blank query matches everything"),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "All four happen to agree today, and the battery still declares the answer per "
            "backend rather than unifying it, because which behaviour is right is a product "
            "question, not a defect. Declaring it keeps the agreement visible without "
            "promising a guarantee the plane has not actually made — and the same battery "
            "asserts nothing about stemming, scoring or tie-break order, which are the "
            "engine's business and which a differential that unified them would get wrong."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_search_conformance.py::"
            "test_search_battery[check_a_blank_query_is_declared_not_guessed]"
        ),
    ),
)


SEARCH_WRITE_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="search_write",
        name="delete-all-on-an-unprovisioned-index",
        observed=(
            EngineBehaviour(engine="mock", behaviour="silently succeeded"),
            EngineBehaviour(engine="meilisearch", behaviour="raised index_not_found"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "The maintenance docs recommend exactly this workflow — wipe first, then rebuild "
            "into the empty index — so the recommended path broke on a fresh deployment and "
            "passed only against the oracle. delete_all is a postcondition: an absent index "
            "already holds no documents. The tolerance is per-call rather than global, "
            "because the same error from an upsert still means the write went nowhere."
        ),
        probe=(
            "tests/integration/test_forze_meilisearch/test_meilisearch_conformance.py::"
            "test_search_write_battery[check_delete_all_on_an_unprovisioned_index_is_a_no_op]"
        ),
    ),
)


# ....................... #


GRAPH_MANAGEMENT_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="graph_management",
        name="duplicate-vertex-key-without-a-schema",
        observed=(
            EngineBehaviour(engine="mock", behaviour="raised graph_vertex_conflict"),
            EngineBehaviour(
                engine="neo4j",
                behaviour=(
                    "silently created a SECOND node under the same key, so the key stopped "
                    "addressing one thing"
                ),
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Cypher CREATE cannot enforce uniqueness; only the constraint that ensure_schema "
            "installs can. With the schema Neo4j does refuse, but as a generic conflict, so "
            "the adapter remaps it to the plane's own code. The lasting change is to the "
            "contract: ensure_schema is documented as required for CORRECTNESS, not as a "
            "performance step somebody may skip."
        ),
        probe=(
            "tests/integration/test_forze_neo4j/test_neo4j_mock_conformance.py::"
            "test_mock_matches_neo4j_duplicate_key_conflict"
        ),
    ),
    PlaneDivergence(
        plane="graph_management",
        name="typed-property-filter-values",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour="returned 0 matches for a UUID filter value — 'no such vertex'",
            ),
            EngineBehaviour(engine="neo4j", behaviour="the driver rejected the parameter type"),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Properties are stored via model_dump(mode='json'), so a UUID is stored as a "
            "string, but filter values were passed raw on both sides. The mock's answer was "
            "the dangerous one: an empty result reads as an absent vertex rather than as a "
            "type error. Both adapters now normalise filter values through the same helper "
            "that produced the stored form — a value-normalisation omission is one bug per "
            "backend, not one bug."
        ),
        probe=(
            "tests/integration/test_forze_neo4j/test_neo4j_mock_conformance.py::"
            "test_mock_matches_neo4j_typed_properties_and_filters"
        ),
    ),
)


# ....................... #


DELIVERY_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="delivery",
        name="uncommitted-outbox-row-visibility",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "a concurrent relay CAN read another transaction's not-yet-committed "
                    "outbox rows — the journal writes through instead of buffering"
                ),
            ),
            EngineBehaviour(
                engine="postgres",
                behaviour="READ COMMITTED prevents it; the relay sees nothing until commit",
            ),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "Deliberate, and the sharpest example of why the horizon needs naming. Whole-store "
            "snapshot isolation for the outbox would serialise concurrent transactions and "
            "blind DST to the interleavings it exists to explore. Atomicity still holds — a "
            "rolled-back transaction leaves no rows, so no double-publish-from-abort finding "
            "can come from this — but a premature-visibility finding on the outbox path may be "
            "mock over-visibility and must be confirmed against a real store. Checked from "
            "both ends rather than asserted: the probe proves the mock over-permits AND that "
            "real Postgres prevents it."
        ),
        probe=(
            "tests/integration/test_forze_postgres/test_pg_delivery_conformance.py::"
            "TestPostgresOutboxOverVisibility::test_relay_cannot_see_uncommitted_row_on_real_postgres"
        ),
    ),
)


# ----------------------- #


PLANE_DIVERGENCES: dict[str, tuple[PlaneDivergence, ...]] = {
    "counter": COUNTER_DIVERGENCES,
    "storage": STORAGE_DIVERGENCES,
    "inference": INFERENCE_DIVERGENCES,
    "search": SEARCH_DIVERGENCES,
    "search_write": SEARCH_WRITE_DIVERGENCES,
    "graph_management": GRAPH_MANAGEMENT_DIVERGENCES,
    "delivery": DELIVERY_DIVERGENCES,
}
"""Every catalogued divergence, by plane. The isolation family lives in ``divergence.py``."""
