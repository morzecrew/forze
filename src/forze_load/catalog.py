"""Operation references resolved from the app's OpenAPI document (RFC 0084 §5.5).

A plan names operations by key; this module finds each one's method and path in the
document, checks the declared ``writes`` against the route's ``x-forze-kind``, generates
the read sweep, and holds the ids sampled for each principal. Everything here runs before
any sign-in, except sampling, which runs as each principal before the first step.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import attrs
import httpx

from forze.base.exceptions import exc

from .plan import (
    Call,
    LoadPlan,
    OpScenario,
    PlanScenario,
    ReadSweep,
    Sample,
    Scenario,
    SendFn,
)

# ----------------------- #

KIND_EXTENSION = "x-forze-kind"
"""The vendor key ``forze_fastapi`` puts on every governed route: ``query`` or ``command``."""

QUERY = "query"
COMMAND = "command"

_ID = "id"
_SIZE = "size"
_METHODS = frozenset({"get", "put", "post", "delete", "options", "head", "patch", "trace"})
_PLACEHOLDER = re.compile(r"\{([^}/]+)\}")


class SampleExhausted(Exception):
    """A call picked from a sample that 404s emptied while it waited; it is not sent."""


# ....................... #


@attrs.frozen(kw_only=True)
class _Route:
    """One operation as the OpenAPI document describes it."""

    operation: str
    method: str
    path: str
    declared: Any
    """The raw ``x-forze-kind``, for messages; ``None`` when absent."""

    path_params: frozenset[str]
    query_params: frozenset[str]
    required_query: frozenset[str]
    has_body: bool
    body_required: bool
    body_json: bool
    body_fields: frozenset[str]
    body_requires: tuple[str, ...]
    body_composed: bool
    """The body schema is an ``allOf``, ``anyOf`` or ``oneOf``: what it requires is not read."""

    @property
    def kind(self) -> str:
        """``query`` only when the route says so; anything else counts as ``command``."""

        return QUERY if self.declared == QUERY else COMMAND

    def unsendable(self, *, allow_id: bool) -> str | None:
        """Why the route cannot be called with no input (with only an id if *allow_id*)."""

        needed = (self.path_params | self.required_query) - ({_ID} if allow_id else set())

        if needed:
            return f"needs {sorted(needed)}"

        if self.body_required and not self.body_json:
            return "takes a body that is not JSON"

        if self.has_body and self.body_composed:
            return "its body schema is composed (allOf, anyOf or oneOf)"

        if self.body_requires:
            return f"its body requires {list(self.body_requires)}"

        return None


@attrs.define
class SamplePool:
    """Sampled values per principal and :class:`~forze_load.Sample`, for one run.

    Values that answer 404 leave the pool. When a principal's values for a sample run out,
    :attr:`on_exhausted` is told once.
    """

    on_exhausted: Callable[[str, Sample], None] | None = None
    _values: dict[tuple[str, Sample], list[Any]] = attrs.field(factory=dict)

    def put(self, principal: str, sample: Sample, values: list[Any]) -> None:
        self._values[(principal, sample)] = values

    def pick(self, principal: str, sample: Sample, rng: random.Random) -> Any:
        values = self._values.get((principal, sample))

        if not values:
            raise SampleExhausted

        return rng.choice(values)

    def drop(self, principal: str, sample: Sample, value: Any) -> None:
        values = self._values.get((principal, sample))

        # Another call may have dropped it first; only the call that empties it reports.
        if not values or value not in values:
            return

        values.remove(value)

        if not values and self.on_exhausted is not None:
            self.on_exhausted(principal, sample)


@attrs.frozen(kw_only=True)
class Catalog:
    """A plan's scenarios for one run, with operation references resolved."""

    scenarios: list[Scenario]
    """Every scenario the run picks from, groups expanded."""

    members: dict[str, list[str]]
    """Each name a mix may use, to the scenarios it picks between."""

    samples_of: dict[str, frozenset[Sample]]
    """The samples each scenario's requests draw from."""

    described: dict[str, dict[str, Any]]
    """Each scenario as the meta line records it."""

    skipped: dict[str, str]
    """Query operations a read sweep matched and could not generate, with the reason."""

    openapi_sha256: str | None
    """Digest of the document the references were resolved from; ``None`` if none was read."""

    pool: SamplePool
    _sample_routes: dict[Sample, _Route]

    # ....................... #

    def expand(self, mix: Mapping[str, int]) -> dict[str, float]:
        """A principal's mix over scenarios: a group's weight is split between its members."""

        weights: dict[str, float] = {}

        for name, weight in mix.items():
            members = self.members[name]

            for member in members:
                # One member keeps its integer weight, so plain picks match P1's exactly.
                weights[member] = weights.get(member, 0) + (
                    weight if len(members) == 1 else weight / len(members)
                )

        return weights

    async def collect(
        self,
        client: httpx.AsyncClient,
        sample: Sample,
        principal: str,
        headers: Mapping[str, str],
    ) -> None:
        """Read *sample* as *principal* into the pool.

        :raises CoreException: ``configuration`` — ``load_sample_failed`` when the list
            operation fails or answers no list of records, ``load_sample_empty`` when no
            record carries the field. Messages name the principal and the operation, never
            a value.
        """

        route = self._sample_routes[sample]
        body = (
            ({_SIZE: sample.size} if _SIZE in route.body_fields else {}) if route.has_body else None
        )
        params = {_SIZE: sample.size} if body is None and _SIZE in route.query_params else None
        where = f"Sampling {sample.operation!r} as {principal!r}"

        try:
            response = await client.request(
                route.method, route.path, json=body, params=params, headers=dict(headers)
            )

        except httpx.TransportError as error:
            raise _sample_failed(f"{where} failed: {type(error).__name__}") from error

        if not 200 <= response.status_code < 300:
            raise _sample_failed(f"{where} answered HTTP {response.status_code}")

        try:
            data = response.json()

        except ValueError:
            data = None

        records = data.get("hits") if isinstance(data, dict) else data

        if not isinstance(records, list):
            raise _sample_failed(f"{where} answered no list of records (a 'hits' list or an array)")

        found = (record.get(sample.field) for record in records if isinstance(record, dict))
        values = list(dict.fromkeys(v for v in found if _is_id(v)))

        if not values:
            raise exc.configuration(
                f"{where} found no {sample.field!r}: its scenarios would have no id to call",
                code="load_sample_empty",
            )

        self.pool.put(principal, sample, values)


# ....................... #


async def resolve(plan: LoadPlan, client: httpx.AsyncClient) -> Catalog:
    """Resolve *plan*'s operation references against its OpenAPI document.

    A plan with none reads no document and keeps its scenarios as they are.

    :raises CoreException: ``configuration`` with one of these codes:

        - ``load_openapi_unavailable`` — the document cannot be fetched, read or parsed, or
          an operation in it is malformed;
        - ``load_op_missing`` / ``load_op_ambiguous`` — an operation is not in the document,
          or is in it more than once; a read sweep that generates nothing is missing too;
        - ``load_op_mismatch`` — an operation's kind contradicts the scenario's ``writes``,
          or a sample reads an operation that is not a ``query``;
        - ``load_plan_invalid`` — a reference's path, query or body does not fit its route,
          or the base URL lacks the path of the document's first server.
    """

    pool = SamplePool()
    references = [s for s in plan.scenarios if isinstance(s, OpScenario | ReadSweep)]

    if not references:
        scenarios = [s for s in plan.scenarios if isinstance(s, Scenario)]

        return Catalog(
            scenarios=scenarios,
            members={s.name: [s.name] for s in scenarios},
            samples_of={},
            described={s.name: {"writes": s.writes} for s in scenarios},
            skipped={},
            openapi_sha256=None,
            pool=pool,
            sample_routes={},
        )

    document = await _read_document(plan.openapi, client)
    _check_server(document, plan.base_url)
    builder = _Builder(index=_index(document), pool=pool, taken={s.name for s in plan.scenarios})

    for scenario in plan.scenarios:
        builder.add(scenario)

    return Catalog(
        scenarios=builder.scenarios,
        members=builder.members,
        samples_of=builder.samples_of,
        described=builder.described,
        skipped=builder.skipped,
        openapi_sha256=hashlib.sha256(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        pool=pool,
        sample_routes=builder.sample_routes,
    )


# ....................... #


@attrs.define(kw_only=True)
class _Builder:
    index: dict[str, list[_Route]]
    pool: SamplePool
    taken: set[str]
    """Names the plan gives its own scenarios: a sweep never generates one of them."""

    scenarios: list[Scenario] = attrs.field(factory=lambda: list[Scenario]())
    members: dict[str, list[str]] = attrs.field(factory=dict)
    samples_of: dict[str, frozenset[Sample]] = attrs.field(factory=dict)
    described: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    skipped: dict[str, str] = attrs.field(factory=dict)
    sample_routes: dict[Sample, _Route] = attrs.field(factory=dict)

    def add(self, scenario: PlanScenario) -> None:
        if isinstance(scenario, Scenario):
            self._keep(scenario, frozenset(), {"writes": scenario.writes})

        elif isinstance(scenario, OpScenario):
            self.members[scenario.name] = [scenario.name]
            self._op(scenario, group=None)

        else:
            self._sweep(scenario)

    # ....................... #

    def _keep(
        self, scenario: Scenario, samples: frozenset[Sample], described: dict[str, Any]
    ) -> None:
        self.scenarios.append(scenario)
        self.members.setdefault(scenario.name, [scenario.name])
        self.samples_of[scenario.name] = samples
        self.described[scenario.name] = described

    def _op(self, op: OpScenario, *, group: str | None) -> None:
        route = _find(self.index, op.operation)
        declared = COMMAND if op.writes else QUERY

        if route.kind != declared:
            raise exc.configuration(
                f"Scenario {op.name!r} declares writes={op.writes}, but operation "
                f"{op.operation!r} is {_kind_text(route)}; an absent or unknown "
                f"{KIND_EXTENSION} counts as {COMMAND}",
                code="load_op_mismatch",
            )

        _check_inputs(op, route)
        samples = frozenset(
            value for value in (*op.path.values(), *op.params.values()) if isinstance(value, Sample)
        )

        for sample in samples:
            self.sample_routes[sample] = _sample_route(self.index, sample)

        described: dict[str, Any] = {"writes": op.writes, "operation": op.operation}

        if group is not None:
            described["group"] = group

        send = _send(route, op, self.pool)
        self._keep(Scenario(op.name, send, writes=op.writes), samples, described)

    def _sweep(self, sweep: ReadSweep) -> None:
        generated: list[str] = []

        for key, routes in self.index.items():
            # A key listed twice with one query among them is skipped, with its reason, below.
            if not key.startswith(sweep.prefix) or not any(r.kind == QUERY for r in routes):
                continue

            reason = self._unreadable(key, routes)

            if reason is not None:
                self.skipped[key] = reason
                continue

            route = routes[0]
            id_sample = (
                Sample(_sibling_list(key))
                if _ID in route.path_params | route.required_query
                else None
            )
            ids = {_ID: id_sample} if id_sample is not None else {}
            self.taken.add(key)
            self._op(
                OpScenario(
                    operation=key,
                    name=key,
                    writes=False,
                    path=ids if _ID in route.path_params else {},
                    params=ids if _ID not in route.path_params else {},
                    body={} if route.body_required else None,
                ),
                group=sweep.name,
            )
            generated.append(key)

        if not generated:
            raise exc.configuration(
                f"Scenario.reads(prefix={sweep.prefix!r}) generated no scenario "
                f"({len(self.skipped)} skipped in all)",
                code="load_op_missing",
            )

        self.members[sweep.name] = generated

    def _unreadable(self, key: str, routes: list[_Route]) -> str | None:
        """Why the sweep cannot generate *key*, or ``None`` when it can."""

        if len(routes) > 1:
            return f"appears {len(routes)} times in the document"

        if key in self.taken:
            return "named by another scenario"

        route = routes[0]
        reason = route.unsendable(allow_id=True)

        if reason is not None:
            return reason

        if _ID not in route.path_params | route.required_query:
            return None

        lister = _sibling_list(key)
        listing = self.index.get(lister, [])

        if len(listing) != 1 or listing[0].kind != QUERY or listing[0].unsendable(allow_id=False):
            return f"no {lister!r} query to sample ids from"

        return None


# ....................... #


async def _read_document(
    source: str | Path | Mapping[str, Any], client: httpx.AsyncClient
) -> dict[str, Any]:
    document: Any

    if isinstance(source, Mapping):
        document = dict(source)

    elif isinstance(source, Path):
        # Off the loop: a document of a few megabytes is read once, before any load.
        document = await asyncio.to_thread(_read_file, source)

    else:
        try:
            response = await client.get(source)

        except httpx.TransportError as error:
            raise _unavailable(f"GET {source} failed: {type(error).__name__}") from error

        if response.status_code != 200:
            raise _unavailable(
                f"GET {source} answered HTTP {response.status_code}; for a deployment that "
                "serves no docs, pass the document as a file (openapi=Path(...))"
            )

        try:
            document = response.json()

        except ValueError as error:
            raise _unavailable(f"GET {source} answered something that is not JSON") from error

    if not isinstance(document, dict) or not isinstance(document.get("paths"), dict):
        raise _unavailable("The OpenAPI document has no 'paths' object")

    return document


def _read_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))

    except (OSError, ValueError) as error:
        raise _unavailable(f"{path} cannot be read as JSON ({type(error).__name__})") from error


def _index(document: Mapping[str, Any]) -> dict[str, list[_Route]]:
    """Every operation with an ``operationId``, in document order."""

    index: dict[str, list[_Route]] = {}

    for path, raw in document["paths"].items():
        item = _deref(document, raw)
        shared = item.get("parameters", [])

        if not isinstance(shared, list):
            raise _unavailable(f"Path {path!r} has 'parameters' that are not a list")

        for method, operation in item.items():
            if method not in _METHODS or not isinstance(operation, dict):
                continue

            key = operation.get("operationId")

            if isinstance(key, str):
                index.setdefault(key, []).append(
                    _route(document, key, method, path, operation, shared)
                )

    return index


def _route(
    document: Mapping[str, Any],
    key: str,
    method: str,
    path: str,
    operation: Mapping[str, Any],
    shared: Sequence[Any],
) -> _Route:
    own = operation.get("parameters", [])
    raw_body = operation.get("requestBody")

    if not isinstance(own, list):
        raise _malformed(key, "'parameters' that are not a list")

    parameters = [_deref(document, p) for p in (*shared, *own)]

    if not all(isinstance(p.get("name"), str) and isinstance(p.get("in"), str) for p in parameters):
        raise _malformed(key, "a parameter without a name or a location")

    if raw_body is not None and not isinstance(raw_body, dict):
        raise _malformed(key, "a 'requestBody' that is not an object")

    query = [p for p in parameters if p["in"] == "query"]
    body = _deref(document, raw_body)
    content = body.get("content", {})

    if not isinstance(content, dict):
        raise _malformed(key, "a body 'content' that is not an object")

    media = content.get("application/json")
    schema = _deref(document, media.get("schema") if isinstance(media, dict) else None)
    fields = schema.get("properties")
    requires = schema.get("required")

    return _Route(
        operation=key,
        method=method.upper(),
        path=path,
        declared=operation.get(KIND_EXTENSION),
        path_params=frozenset(_PLACEHOLDER.findall(path)),
        query_params=frozenset(p["name"] for p in query),
        required_query=frozenset(p["name"] for p in query if p.get("required")),
        has_body=bool(body),
        body_required=bool(body.get("required")),
        body_json="application/json" in content,
        body_fields=frozenset(fields) if isinstance(fields, dict) else frozenset(),
        body_requires=tuple(r for r in requires if isinstance(r, str))
        if isinstance(requires, list)
        else (),
        body_composed=any(k in schema for k in ("allOf", "anyOf", "oneOf")),
    )


def _check_server(document: Mapping[str, Any], base_url: str) -> None:
    """Refuse a document whose paths live under a server path the base URL lacks.

    FastAPI behind a ``root_path`` lists it as the first server (``/api/v1``) and keeps the
    paths without it. A base URL that misses it would send every request to a path that
    404s, which reads as deleted data, not as a misconfiguration.
    """

    servers = document.get("servers")
    first = servers[0] if isinstance(servers, list) and servers else None
    url = first.get("url") if isinstance(first, dict) else None

    if not isinstance(url, str) or not url.startswith("/"):
        return  # none, or an absolute server: the base URL decides

    prefix = url.rstrip("/")

    if prefix and not urlsplit(base_url).path.rstrip("/").endswith(prefix):
        raise _invalid(
            f"The OpenAPI document serves its paths under {prefix!r}, and base_url does not "
            "end with it; add it to base_url"
        )


def _deref(document: Mapping[str, Any], node: Any) -> dict[str, Any]:
    """*node* with a local ``$ref`` (``#/components/...``) followed, or ``{}``."""

    for _ in range(8):  # a chain of refs ends quickly; a cycle must not hang the start
        if not isinstance(node, dict):
            return {}

        ref = node.get("$ref")

        if not isinstance(ref, str) or not ref.startswith("#/"):
            return node

        target: Any = document

        for part in ref[2:].split("/"):
            target = target.get(part) if isinstance(target, dict) else None

        node = target

    return {}


def _find(index: Mapping[str, list[_Route]], key: str) -> _Route:
    routes = index.get(key, [])

    if not routes:
        raise exc.configuration(
            f"Operation {key!r} is not in the OpenAPI document", code="load_op_missing"
        )

    if len(routes) > 1:
        raise exc.configuration(
            f"Operation {key!r} appears {len(routes)} times in the OpenAPI document",
            code="load_op_ambiguous",
        )

    return routes[0]


def _sample_route(index: Mapping[str, list[_Route]], sample: Sample) -> _Route:
    route = _find(index, sample.operation)

    if route.kind != QUERY:
        raise exc.configuration(
            f"sample({sample.operation!r}) reads an operation that is {_kind_text(route)}; "
            f"a sample only reads, so it must be a {QUERY}",
            code="load_op_mismatch",
        )

    reason = route.unsendable(allow_id=False)

    if reason is not None:
        raise _invalid(f"sample({sample.operation!r}) cannot call its operation: it {reason}")

    return route


def _check_inputs(op: OpScenario, route: _Route) -> None:
    where = f"Scenario {op.name!r} ({op.operation})"
    given = set(op.path)

    if given != route.path_params:
        raise _invalid(
            f"{where} fills path {sorted(given)}, the route's placeholders are "
            f"{sorted(route.path_params)}"
        )

    unknown = sorted(set(op.params) - route.query_params)
    missing = sorted(route.required_query - set(op.params))

    if unknown or missing:
        raise _invalid(f"{where} has unknown query {unknown} and lacks required query {missing}")

    if op.body is None and route.body_required:
        raise _invalid(f"{where} needs a body")

    if op.body is not None and not route.has_body:
        raise _invalid(f"{where} sends a body to a route that takes none")


def _send(route: _Route, op: OpScenario, pool: SamplePool) -> SendFn:
    """The request an operation reference sends, with sampled values picked per call."""

    async def send(call: Call) -> httpx.Response:
        picked: list[tuple[Sample, Any]] = []

        def fill(value: Any) -> Any:
            if not isinstance(value, Sample):
                return value

            chosen = pool.pick(call.principal, value, call.rng)
            picked.append((value, chosen))

            return chosen

        path = route.path

        for name, value in op.path.items():
            path = path.replace("{" + name + "}", quote(str(fill(value)), safe=""))

        params = {name: fill(value) for name, value in op.params.items()}
        response = await call.request(route.method, path, json=op.body, params=params or None)

        # Deleted, or gone for this principal: the value leaves the sample, not the run.
        if response.status_code == 404:
            for sample, value in picked:
                pool.drop(call.principal, sample, value)

        return response

    return send


def _sibling_list(key: str) -> str:
    namespace, dot, _ = key.rpartition(".")

    return f"{namespace}{dot}list"


def _kind_text(route: _Route) -> str:
    return f"{KIND_EXTENSION}: {route.declared!r}" if route.declared is not None else "unmarked"


def _is_id(value: Any) -> bool:
    return isinstance(value, str | int) and not isinstance(value, bool)


def _invalid(summary: str) -> Exception:
    return exc.configuration(summary, code="load_plan_invalid")


def _unavailable(summary: str) -> Exception:
    return exc.configuration(summary, code="load_openapi_unavailable")


def _malformed(key: str, what: str) -> Exception:
    return _unavailable(f"Operation {key!r} in the OpenAPI document has {what}")


def _sample_failed(summary: str) -> Exception:
    return exc.configuration(summary, code="load_sample_failed")
