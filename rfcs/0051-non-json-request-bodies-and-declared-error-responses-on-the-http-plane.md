# RFC 0051 — Non-JSON request bodies and declared error responses on the HTTP plane

- **Status:** 📝 Draft
- **Scope:** Three additive gaps on the outbound HTTP service plane, each of which today forces an app off the governed path and onto a bare client: a request body can only be JSON, client authentication cannot be HTTP Basic, and a 4xx response body is discarded before a caller can read it. Adds `body_encoding` and `error_type` to `HttpOperationSpec`, a `basic` kind to `HttpAuthConfig`, and a `data=` parameter to `HttpClientPort.request`. Contract change is two optional fields with defaults that reproduce today's behaviour exactly; no existing spec, config or call changes meaning. **Not** a general content-negotiation layer (no multipart, no XML, no protobuf), not a retry or error-classification policy, and not a change to how errors are logged or exposed — the declared error body rides the sanitization path that already exists.
- **Related:** [`contracts/http/specs.py`](../src/forze/application/contracts/http/specs.py) (the two new fields), [`integrations/http/parts.py`](../src/forze/application/integrations/http/parts.py) (body assembly), [`forze_http/adapters/http_service.py`](../src/forze_http/adapters/http_service.py) (the call and the error path), [`forze_http/kernel/client/errors.py`](../src/forze_http/kernel/client/errors.py) (the mapper that drops the body today), [`forze_http/execution/deps/configs.py`](../src/forze_http/execution/deps/configs.py) (`HttpAuthConfig`), [`base/exceptions/envelope.py`](../src/forze/base/exceptions/envelope.py) (the sanitized `details` → `context` path §5.3 rides). RFC 0024 (the blocked consumer), RFC 0022 (the egress marker that makes a *declared service* the governed unit, and therefore makes these gaps load-bearing rather than cosmetic).
- **Origin:** RFC 0024 P1 halted on decision 7 — see [`logs/T-0024.md`](../logs/T-0024.md). Its token-endpoint client has to be a declared `forze_http` service to carry the `egress_sensitive` marker, and a declared service could not POST a form body, could not present `client_secret_basic`, and could not tell `invalid_grant` from a transient 400. The three blockers are general, not OAuth-specific, which is why they are one RFC on the plane rather than three carve-outs in a kit.

---

## 1. Summary

`HttpOperationSpec` gains `body_encoding` (`"json"` by default, `"form"` for
`application/x-www-form-urlencoded`) and `error_type` (a model an operation declares for
its error responses). `HttpAuthConfig` gains `kind="basic"` with `username`/`password`.
`HttpClientPort.request` gains a keyword-only `data=`. A form body accepts scalars only
and refuses anything else by name; a declared error body is validated and carried in
`CoreException.details`, so the scrubber and the egress policy that already govern
`details` govern it too. Every default reproduces current behaviour.

## 2. Motivation

An OAuth 2.0 token endpoint is the sharpest case and not a special one: RFC 6749 §4.1.3
**requires** `application/x-www-form-urlencoded`, §2.3.1 says clients SHOULD use HTTP
Basic and that servers MAY refuse anything else, and §5.2 returns `invalid_grant` as a
JSON body on a 400. The plane can express none of the three.

What an app does instead is reach for `httpx` directly, and the cost is not theoretical —
this repository already contains that workaround. The inbound IdP presets POST their token
exchange from a bare `httpx.AsyncClient` in
[`builtin/idp/_exchange.py`](../src/forze_identity/builtin/idp/_exchange.py), which means
no tenant routing, no deadline propagation, no resilience, no per-port span, and — since
RFC 0022 — no egress marker, because a bare client has no service config to declare
anything on. Each gap individually is an inconvenience; together they mean the one class of
outbound call that carries credentials across the trust boundary is the class that cannot
be governed.

The third gap is the one that silently corrupts rather than merely blocks.
`CredentialExchangerPort` requires its implementer to distinguish a permanent rejection
(raise `INVALID_GRANT_CODE`, the store records a burn notice) from a transient failure
(raise anything else, the credential survives), and says in as many words that reporting
transient as permanent destroys a working credential. Today every 4xx becomes
`exc.infrastructure("HTTP client error (400).")` with the body dropped, so an implementer
must choose between never recording a rejection and burning a grant on any malformed
request.

## 3. Current state

Verified against the tree at `e0d43055d`:

- **One encoding.** `HttpServiceAdapter.invoke` calls `self.client.request(..., json=body)`
  — the only body parameter that exists, on the adapter and on `HttpClientPort` alike.
  `request_parts` returns a `JsonDict` body and knows nothing about encodings.
- **No Basic.** `HttpAuthConfig.kind` is `Literal["bearer", "api_key", "header"]` with a
  single `token: SecretStr`. `auth_headers()` builds one header from it.
- **The body is dropped on 4xx.** `_httpx_eh` maps 404/401/403/429/5xx to their kinds and
  everything else to `exc.infrastructure(f"HTTP client error ({status}).")`. It receives an
  `httpx.HTTPStatusError`, which *has* the response — the body is available at that point
  and discarded.
- **`details` is already the sanitized channel.** `error_envelope` runs `details` through
  the scrubber and exposes it as `context` only when the per-kind egress policy allows,
  rendering a closed set of leaf types. §5.3 needs no new machinery; it needs to put
  declared fields into the channel that exists.
- **No test doubles to widen.** Nothing in `src/` or `tests/` implements `HttpClientPort`
  except `HttpClient` and `RoutedHttpClient`; the adapter's tests drive the real client
  over `httpx.MockTransport`, which is what makes §6's assertions about real bytes possible.

## 4. Goals / Non-goals

**Goals**

- A declared service can POST `application/x-www-form-urlencoded`, per operation.
- A declared service can present HTTP Basic client authentication.
- An operation can declare the shape of its error responses and a caller can read the
  declared fields, with no change to how errors are logged or exposed.
- Every existing spec, config and call behaves identically with no edit.

**Non-goals**

- **A content-negotiation layer.** No multipart, XML, protobuf, or per-request override.
  Two encodings cover the plane's actual traffic; a third arrives when a consumer needs it.
- **Error classification policy.** The plane hands a caller the declared fields; deciding
  that `invalid_grant` is permanent belongs to the caller, because only it knows what its
  counterparty's codes mean.
- **Call-time secret resolution for service auth.** §5.2 explains why this is a separate,
  larger question than the one this RFC answers.
- **Changing the error mapper's kinds or codes.** A 400 stays `infrastructure`; what
  changes is that a declared body travels with it.

## 5. Design

### 5.1 `body_encoding` — per operation, scalars only

```python
class HttpOperationSpec(Generic[In, Out]):
    body_encoding: Literal["json", "form"] = "json"
```

Per **operation**, not per service: a provider's token endpoint is form-encoded while its
data API is JSON, and they are one service with one base URL and one auth. Per-service
would force an app to declare two services for one provider and keep their configs in sync.

`request_parts` assembles the same body mapping it does today. For `"form"` the adapter
passes it as `data=`, and the mapping must be flat: `str`, `int`, `float`, `bool` and
`None` are accepted (`None` is omitted, `bool` becomes `"true"`/`"false"` — the spelling
every OAuth and webhook endpoint expects), and any other value raises `exc.validation`
naming the field. `application/x-www-form-urlencoded` has no nesting, so the alternative
to refusing is letting `httpx` stringify a `dict` into `"{'a': 1}"` and sending something
no server can parse — a wrong request that looks like a working one.

`GET` operations are unaffected: they carry no body today and that does not change, so
`body_encoding` on a `GET` is inert rather than an error — the field describes how a body
is encoded, and a `GET` has none.

### 5.2 `basic` — wiring-time credentials, and why not call-time

```python
class HttpAuthConfig:
    kind: Literal["bearer", "api_key", "header", "basic"] = "bearer"
    username: str | None = None
    password: SecretStr | None = None
```

`auth_headers()` emits `Authorization: Basic <base64(username:password)>`. The password is
a `SecretStr` with `repr=False`, like `token` beside it, so it does not reach a log through
a config repr.

RFC 0024 §5 asks for the client secret to be resolved from a `SecretRef` **at call time**.
This RFC deliberately does not do that, and the reason is worth stating because it looks
like a shortcut and is not. The only call-time credential path on this plane resolves
`HttpRoutingCredentials` per **tenant** — it exists so tenant A's integration uses tenant
A's key. An OAuth client secret is the opposite shape: one secret belonging to the
application, used for every tenant's grant. Routing it through the tenant path would key
the app's own credential by whichever tenant happened to connect, and give it a different
value per tenant slot. Wiring-time resolution is also how every other value on
`HttpAuthConfig` already arrives, and an app that resolves secrets at startup is resolving
this one the same way it resolves the rest.

What would justify call-time resolution is a *rotating* client secret — a provider that
expires the client credential itself, not the grant. Nothing in the tree has one; when one
appears, the honest shape is a service-level `SecretRef` resolved per call for every auth
kind at once, not a carve-out for Basic.

### 5.3 `error_type` — declared fields, on the channel that already sanitizes

```python
class HttpOperationSpec(Generic[In, Out]):
    error_type: type[BaseModel] | None = None
```

When an operation declares `error_type` and the response status is ≥ 400, the adapter
validates the body into that model and attaches its dump to the raised exception's
`details` under the reserved key `response_error`. The kind, the code and the summary are
exactly what they are today; a caller that ignores `details` sees no change.

Three properties make this the right channel rather than a new one:

- **Only declared fields travel.** The app names the model, so a body's unlisted fields
  never enter the exception. Attaching the raw body instead would mint text the scrubber
  never vetted, from a response the app has not described — the thing
  [`envelope.py`](../src/forze/base/exceptions/envelope.py) refuses to do with a blanket
  `default=str`, for the same reason.
- **The existing egress policy still decides exposure.** `details` reaches a client only
  as sanitized `context`, and only when the per-kind policy allows. A declared error body
  inherits that, unchanged.
- **A malformed error body is not a second failure.** If the body does not validate, the
  adapter raises what it would have raised anyway. An error response that is itself
  broken must not replace the error the caller was already getting — that is the
  "cleanup must not outrank the outcome" shape, and here the cleanup is optional decoration.

### Alternatives considered

- **A typed `HttpOperationError(CoreException)` carrying the model.** Nicer at the call
  site than a `details` lookup, and it would need the envelope, the egress policy and every
  transport that renders an error to learn about a new class — for a strictly smaller
  guarantee than `details` already provides. Rejected on cost, not on taste; if a caller's
  ergonomics ever justify it, it can wrap the `details` contract without changing it.
- **Returning the error model instead of raising** (`invoke` → `Out | Err`). Changes the
  return type of every call on the plane to satisfy one caller. Rejected.
- **Raw body in `details`.** Rejected above: undeclared passthrough of a response body
  into the sanitization path is how PII reaches a log.
- **Per-service `body_encoding`.** Rejected in §5.1: one provider, two encodings.

## 6. Tests

The real leg is `httpx.MockTransport`, which hands the test the actual `httpx.Request` —
so the assertions are on the bytes and headers that went out, not on what the adapter
intended. That distinction matters here: an encoding bug is exactly the kind that passes a
test written against the adapter's own inputs.

- A `"form"` operation sends `application/x-www-form-urlencoded` with a urlencoded body,
  and a `"json"` operation on the same service still sends JSON with its own content type.
- A non-scalar field in a form body raises `exc.validation` naming the field, before any
  request is made.
- `bool` and `None` serialize as `"true"`/`"false"` and omission — pinned, because a
  provider reading `"True"` fails in a way that looks like a credential problem.
- `kind="basic"` produces the exact `Authorization` header for a known credential pair, and
  the password appears in neither the config's `repr` nor the request's logs.
- A declared error model on a 400 arrives in `details["response_error"]`; an operation with
  no `error_type` raises byte-identically to today; a 400 whose body does not match the
  declared model raises the same exception as if nothing were declared.
- `data=` reaches `RoutedHttpClient`'s inner client, so a tenant-routed form POST works.
- Docs: the `integrations/http.md` snippets stay compiling under the docs-snippet gate.

## 7. Docs

`pages/docs/integrations/http.md` gains the two fields and the Basic kind in the bullets it
already keeps for this plane, with one sentence on the scalars-only rule and one on what
`error_type` does and does not expose. The changelog entry names all three gaps as one
user-facing addition, since an app hits them together.

## 8. Out of scope

- **Multipart and file upload.** Named as the next encoding a real consumer would ask for;
  it needs a streaming body and a filename contract, which is a different design.
- **`client_secret_jwt` / `private_key_jwt`.** Token-endpoint auth methods beyond Basic and
  POST; they need signing keys and belong with whatever ships JWT client assertions.
- **A retry policy keyed on a declared error code.** The resilience plane owns retries; a
  caller that wants "retry on `slow_down`" writes that where its policy lives.

## 9. Risks

- **`error_type` read as a general body-capture feature.** It is not: it captures the
  fields an app declared, on the sanitized channel. The docs say so, and the field name
  says *error*.
- **A form body silently mis-serializing a value.** Mitigated by refusing non-scalars and
  by pinning the `bool`/`None` spelling; the residual risk is a provider expecting
  `"1"`/`"0"`, which the app expresses by typing the field as `str`.
- **Basic auth landing in a log through the header.** The password is a `SecretStr` and the
  header is built at call time; the existing scrubbing applies to headers the adapter logs
  (it logs none today). The test asserts the absence rather than trusting it.
- **Two optional fields becoming four.** Each additive field on a core contract is
  permanent. The two here are justified by a blocked RFC and a workaround already in the
  tree; §8 names what is refused so the field list does not grow by habit.

## 10. Unresolved questions

- Whether `"form"` should also accept a sequence value for a repeated key
  (`scope=a&scope=b`). OAuth uses a space-delimited single value, so nothing needs it yet;
  the first consumer that does decides whether the plane spells it as a list or leaves it
  to the app to join.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `ASSUMED` | `body_encoding` is per **operation**, defaulting to `"json"` — one provider legitimately mixes a form-encoded token endpoint with a JSON API, and per-service would force two service declarations with one base URL between them |
| 2 | `LOCKED` | A form body accepts scalars only (`str`/`int`/`float`/`bool`/`None`) and refuses anything else by field name. `application/x-www-form-urlencoded` cannot nest, so the alternative is sending a stringified `dict` that no server parses — a wrong request that looks like a working one |
| 3 | `ASSUMED` | `basic` credentials are resolved at wiring, like every other value on `HttpAuthConfig`. Call-time resolution exists only per **tenant** on this plane, and an OAuth client secret is per-application, so routing it there would key the app's own credential by the connecting tenant. A rotating *client* secret is what would justify revisiting this, for all auth kinds at once rather than for Basic alone |
| 4 | `LOCKED` | A declared error body travels in `CoreException.details` under `response_error`, never as a raw body and never on a new exception class. `details` is already scrubbed and already gated by the per-kind egress policy, so only fields the app declared reach a log or a client — changing this later means teaching the envelope, the egress policy and every error-rendering transport about a new carrier |
| 5 | `LOCKED` | A body that fails to validate against `error_type` changes nothing: the adapter raises exactly what it raises today. An optional decoration must never replace the failure a caller was already being told about |
| 6 | `ASSUMED` | `HttpClientPort.request` gains `data=` keyword-only with a `None` default, so a narrower implementation stays structurally valid until it is actually called with a form body |
| 7 | `ASSUMED` | No multipart, XML, protobuf, `client_secret_jwt`, or error-keyed retry policy. Each is named in §8 with what would bring it in; the plane gains a third encoding against a consumer, not against a hypothetical |

## 12. Phasing

One PR. The two spec fields, the config kind, the client parameter, the tests and the docs
land together: each piece is useless alone, and the consumer that needs them (RFC 0024 P1)
needs all three gaps closed before it can start.
