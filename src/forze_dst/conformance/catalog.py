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

from collections.abc import Mapping
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
    """What each engine was measured doing, in its own terms — agreeing ones included.

    At least two, because one engine's behaviour is not a comparison and nothing can be
    concluded from it. That floor is not a claim they must disagree: rows where every engine
    lands in the same place are as worth pinning as rows where they split, and both shapes
    ship here.
    """

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
        name="absent-container-vs-empty-container",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "had no container concept at all: every accessor reached its bucket "
                    "through setdefault, so the bucket existed the moment anything asked "
                    "and list(missing_ok=False) could not raise — the parameter was "
                    "documented as a no-op"
                ),
            ),
            EngineBehaviour(
                engine="s3",
                behaviour=(
                    "MinIO and floci both raise 'bucket not found' on a listing of an "
                    "absent bucket, and return an empty page for an existing empty one"
                ),
            ),
            EngineBehaviour(
                engine="gcs",
                behaviour="same: refuses on absent, empty page on existing-and-empty",
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Sharper than the usual mock-vs-real gap, and worth reading as its own class: "
            "the contract was not merely untested against the oracle, it was structurally "
            "*inexpressible* there. No test written against that mock could have failed, so "
            "every mock-backed test of missing_ok was vacuously green — including the one "
            "belonging to the re-encryption sweep, whose whole guard is telling a vanished "
            "bucket from an emptied one. Fixing it meant giving the oracle the concept it "
            "lacked (MockState.storage_buckets) before there was anything to compare: reads "
            "never provision, the four documented write paths do, exactly as the real "
            "adapters draw the line. The sweep's own test then had to say which state it "
            "meant, having asserted 'empty' while exercising 'absent'. The unification "
            "goes one step further than the raise: missing_ok lets a caller tolerate an "
            "absent container without being made blind to it, since every backend now "
            "reports StoredObjectPage.container_missing — under per-tenant buckets the "
            "absent case is the normal state of every tenant that has not uploaded yet, "
            "and collapsing it into a plain empty page hid exactly that population."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_storage_conformance.py::"
            "test_storage_battery[check_an_absent_container_is_not_an_empty_one]"
        ),
    ),
    PlaneDivergence(
        plane="storage",
        name="head-cannot-see-the-container",
        observed=(
            EngineBehaviour(engine="s3", behaviour="head of a missing key: not_found either way"),
            EngineBehaviour(engine="gcs", behaviour="head of a missing key: not_found either way"),
            EngineBehaviour(
                engine="mock",
                behaviour="not_found either way — the oracle must not invent the distinction",
            ),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "Measured rather than assumed, and it corrected the design of this leg. A head "
            "of an absent key returns the same not-found whether or not the bucket exists, "
            "on both S3 servers and on GCS, because HeadObject cannot distinguish them — "
            "so the absent/empty distinction lives at `list` and nowhere else. The download "
            "verb does see it, and disagrees across vendors; that is its own row below."
        ),
        probe=(
            "tests/unit/test_forze_dst/test_conformance/test_storage_containers.py::"
            "test_reads_never_provision_a_container"
        ),
    ),
    PlaneDivergence(
        plane="storage",
        name="download-from-an-absent-container",
        observed=(
            EngineBehaviour(
                engine="s3",
                behaviour=(
                    "raises configuration 'bucket not found' — GetObject reports the "
                    "missing container, not the missing key"
                ),
            ),
            EngineBehaviour(
                engine="gcs",
                behaviour="raises a plain not_found, the same answer it gives for a missing key",
            ),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "Two correct-looking answers to one call, so a caller cannot branch on the kind "
            "here portably. Neither is obviously wrong: an absent bucket is a configuration "
            "fault the caller should not retry, while an absent object is precisely what was "
            "asked for — and note the same two backends agree on not-found at `head`, so the "
            "asymmetry is between the read verbs as much as between the vendors. Normalizing "
            "would mean choosing which fact a download reports, which is a contract decision "
            "rather than an adapter bug, so it is declared and both sides are pinned: the day "
            "one is changed the other fails and the choice has to be made deliberately."
        ),
        probe=(
            "tests/integration/test_forze_s3/test_s3_storage_new_ops.py::"
            "test_download_from_an_absent_bucket_reports_the_container"
        ),
    ),
    PlaneDivergence(
        plane="storage",
        name="missing-container-refusal-kind",
        observed=(
            EngineBehaviour(
                engine="s3",
                behaviour=(
                    "raised infrastructure on NoSuchBucket — a structured vendor code, so "
                    "the mapping site always knew exactly which condition it was"
                ),
            ),
            EngineBehaviour(
                engine="gcs",
                behaviour=(
                    "raised infrastructure on a bucket-scoped 404 (URL shape: /b/<bucket> "
                    "rather than /o/<name>) — likewise structurally identified"
                ),
            ),
            EngineBehaviour(
                engine="mock",
                behaviour="raised infrastructure, deferring to the two real backends",
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Not a disagreement between engines — all three agreed, and all three were "
            "wrong in the same direction. exception_egress_policy reads infrastructure as "
            "retryable, so a saga step or a consumer loop that hit an unprovisioned bucket "
            "retried a condition that cannot resolve without an operator, forever. Both "
            "mapping sites already called it a deployment fault in their own comments; only "
            "the kind disagreed. Now configuration on all three: non-retryable, details "
            "still withheld from clients, still HTTP 500 — the client-facing behaviour is "
            "unchanged and only the retry decision moves. Detection needed no error-text "
            "matching, which is what separates this from the counter-overflow row on the "
            "same theme: there the store does the arithmetic and reports it in prose, so "
            "that one stays DECLARED."
        ),
        probe=(
            "tests/integration/test_forze_s3/test_s3_storage_new_ops.py::"
            "test_download_from_an_absent_bucket_reports_the_container"
        ),
    ),
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
        name="oracle-advertises-the-full-capability-surface",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "a route registered without capabilities= advertises "
                    "FULL_INFERENCE_CAPABILITIES: unbounded batches, streaming, "
                    "deterministic — so every capability gate passes"
                ),
            ),
            EngineBehaviour(
                engine="kserve_v2",
                behaviour="declares the wiring's max_batch_size and refuses a batch past it",
            ),
            EngineBehaviour(
                engine="mlflow",
                behaviour="declares the wiring's max_batch_size and refuses a batch past it",
            ),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "Not a defect in the oracle — it genuinely serves all of it — but it "
            "out-capables every backend it stands in for, so a gate that passes against it "
            "can still refuse in production. The registration seam is the fix "
            "(MockInferenceRegistry.on(..., capabilities=…)) and it cannot be made the "
            "default: the mock is also used where no backend is being mirrored at all, and "
            "defaulting to the narrowest surface would refuse features it really does "
            "serve. So the residual is a wiring obligation, and the differential is what "
            "makes forgetting it fail at authoring time: the probe asserts an untold oracle "
            "ACCEPTS the oversized batch a capped backend REFUSES."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_inference_conformance.py::"
            "test_inference_battery[check_an_unmirrored_oracle_diverges_from_a_capped_backend]"
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


GRAPH_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="graph",
        name="which-neighbours-a-bounded-read-returns",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "walks its edge list in insertion order, so a truncated page is the "
                    "OLDEST matching neighbours — measured as ('w0', 'w1')"
                ),
            ),
            EngineBehaviour(
                engine="neo4j",
                behaviour=(
                    "the adjacency LIMIT carries no ORDER BY, so the page is whatever the "
                    "planner reaches first — measured as ('w2', 'w1'), most-recent-first"
                ),
            ),
        ),
        resolution=DivergenceResolution.DECLARED,
        reason=(
            "Both engines return a full page of genuine neighbours; they disagree only on "
            "WHICH ones, and neither answer is wrong. Unifying it would mean an ORDER BY on "
            "the adjacency, which asks Neo4j to sort a whole neighbourhood before truncating "
            "it — the cost falls hardest on exactly the high-degree vertices a limit exists "
            "to protect. So the leg compares cardinality and membership and leaves identity "
            "out, and the contract says a bounded neighbours call returns an arbitrary "
            "subset. Callers who need a deterministic page need an ordered read, not a "
            "bounded one. What is emphatically NOT declared is a short page: filling the "
            "limit from the wanted kind is asserted on both engines, because a page cut "
            "short by a filter is indistinguishable from the end of the neighbourhood."
        ),
        probe=(
            "tests/unit/test_forze_mock/test_mock_graph_conformance.py::"
            "test_bounded_neighbors_fills_the_page"
        ),
    ),
    PlaneDivergence(
        plane="graph",
        name="non-string-key-field",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "keys its store by str(value), so an int-keyed vertex answered every "
                    "keyed read correctly — exists, get, degree, neighbours"
                ),
            ),
            EngineBehaviour(
                engine="neo4j",
                behaviour=(
                    "stores the native int and matches it against the string VertexRef.key "
                    "carries: writes succeeded and find_vertices returned the rows, while "
                    "vertex_exists was false, get_vertex none, vertex_degree 0 and neighbors "
                    "empty — an empty graph, raised as nothing"
                ),
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "Found by building this leg, not by looking for it. VertexRef.key is typed str "
            "and the adapters honour that literally, so a key the store keeps as a number "
            "can never be matched — and the failure is a silent empty read on the engine, "
            "against a mock where everything works. Coercing the string back to the declared "
            "type was rejected: a str key field whose value happens to be '999' would then "
            "be indistinguishable from the integer 999, so the adapter would be guessing. "
            "GraphNodeSpec/GraphEdgeSpec now refuse a key field declared as bool, int or "
            "float at construction (graph_non_string_key_field), the same wiring-time "
            "refusal the sealed key field already gets and for the same reason: no later "
            'point makes it safe. Decimal is not refused: model_dump(mode="json") renders '
            "it as a string, so it lands on Neo4j as STRING and every keyed read resolves — "
            "measured on the same probe that shows an int key landing as INTEGER and "
            "matching nothing. Native-typed keys remain a capability someone could add; "
            "they are not what silently pretending to work was."
        ),
        probe=(
            "tests/unit/test_forze/application/contracts/test_graph.py::"
            "TestKeyFieldTyping::test_a_numeric_key_field_is_refused_at_construction"
        ),
    ),
)


# ....................... #


REALTIME_CURSOR_DIVERGENCES: tuple[PlaneDivergence, ...] = (
    PlaneDivergence(
        plane="realtime_cursor",
        name="capped-replay-interleaved-with-a-live-ack",
        observed=(
            EngineBehaviour(
                engine="mock",
                behaviour=(
                    "delivers a complete newest-cap suffix; the cumulative ack lands on a "
                    "live frame and the trim floor deletes nothing undelivered"
                ),
            ),
            EngineBehaviour(
                engine="postgres",
                behaviour="the same, through a composite (hlc, id) keyset window in real SQL",
            ),
            EngineBehaviour(
                engine="mongo",
                behaviour="the same — a store the mailbox had no coverage against at all before",
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "The three stores agree, which is the point of pinning it: they agree about an "
            "interaction that no simulation schedule reaches, because the race lives in "
            "document-port code rather than in stream code. A cumulative ack is only true "
            "if the delivered prefix was complete, so a replay truncated by the retention "
            "cap plus a live frame acked mid-stream lets the cursor jump the gap and the "
            "trim floor delete what was never sent — silently, with no error anywhere. The "
            "defence is that the cap moves the window START rather than truncating the "
            "read; entries below the floor are a declared, counted retention loss. The "
            "controls reconstruct the fault by truncating the replay and assert the outcome "
            "names it, so the leg cannot pass by checking nothing."
        ),
        probe=(
            "tests/unit/test_forze_kits/integrations/test_realtime_cursor_conformance.py::"
            "test_cursor_replay_battery[check_a_capped_replay_survives_a_live_ack]"
        ),
    ),
    PlaneDivergence(
        plane="realtime_cursor",
        name="tenant-in-the-derived-cursor-id",
        observed=(
            EngineBehaviour(
                engine="postgres",
                behaviour=(
                    "one physical table holds both tenants' cursor rows, so a tenant-blind "
                    "derived id collides on the primary key: the lookup misses while the "
                    "insert hits the other tenant's row"
                ),
            ),
            EngineBehaviour(
                engine="mongo",
                behaviour="the same shape on a shared collection keyed by _id",
            ),
            EngineBehaviour(
                engine="mock",
                behaviour="partitions per tenant, so the collision is not reachable there",
            ),
        ),
        resolution=DivergenceResolution.UNIFIED,
        reason=(
            "The cursor id is derived deterministically (uuid5) so concurrent first-acks for "
            "one device converge on a single row instead of racing two inserts — and that "
            "derivation has to include the tenant, or the org-switcher flow (one principal "
            "present in two tenants) makes two tenants share a read position. Worth its own "
            "row because the oracle cannot demonstrate it: the mock hard-partitions per "
            "tenant, so the collision is unrepresentable there and only the real stores can "
            "show the guarantee holding. The failure mode is also indirect — the advance "
            "loop exhausts its retry budget rather than returning wrong data — so the probe "
            "catches that code specifically instead of waiting for a bad value."
        ),
        probe=(
            "tests/integration/test_forze_postgres/test_pg_realtime_cursor_conformance.py::"
            "test_cursor_replay_battery[check_tenant_cursors_are_independent]"
        ),
    ),
)


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
    "graph": GRAPH_DIVERGENCES,
    "graph_management": GRAPH_MANAGEMENT_DIVERGENCES,
    "delivery": DELIVERY_DIVERGENCES,
    "realtime_cursor": REALTIME_CURSOR_DIVERGENCES,
}
"""Every catalogued divergence, by plane. The isolation family lives in ``divergence.py``."""


def validate_catalog(catalog: Mapping[str, tuple[PlaneDivergence, ...]]) -> None:
    """Refuse a catalog whose rows are filed wrong, at import rather than at review.

    ``PlaneDivergence`` can only check a row against itself; these are the two things that
    are only wrong *in context*. A row filed under a plane it does not name is a row the
    manifest checker resolves against the wrong plane's probes — it reads as coverage of a
    plane nobody measured. Two rows sharing a name inside one plane make the handle they
    exist to provide ambiguous, so a cross-reference points at either of them.
    """

    for plane, rows in catalog.items():
        seen: set[str] = set()

        for row in rows:
            if row.plane != plane:
                raise exc.configuration(
                    "a divergence row is filed under a plane it does not name",
                    details={"filed_under": plane, "names": row.plane, "row": row.name},
                )

            if row.name in seen:
                raise exc.configuration(
                    "two divergence rows in one plane share a name",
                    details={"plane": plane, "name": row.name},
                )

            seen.add(row.name)


validate_catalog(PLANE_DIVERGENCES)
