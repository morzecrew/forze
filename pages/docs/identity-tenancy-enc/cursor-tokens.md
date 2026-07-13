---
title: Cursor tokens
icon: lucide/ticket-check
summary: Signing or sealing keyset pagination cursors — tamper-evidence, confidentiality, and binding a cursor to the query that minted it
---

A [cursor page](../data-events/reading-data.md#pagination-the-method-suffix) hands
the client a token that encodes where the next page starts. That token is
**client-held query state**: by default it is readable base64-JSON — the sort
keys, directions, and the boundary row's sort values — and the client sends it
back verbatim. Nothing stops a client from *editing* it: forging boundary values
to probe rows its filter never matched, or replaying a cursor minted under one
query against another. For a public API that is an integrity gap, and when sort
keys carry values the row projection doesn't return, a confidentiality gap too.

Cursor-token protection closes both, the same way the rest of this section
works: declared once, applied by the runtime everywhere, **off by default** —
and when it is off, tokens are byte-for-byte what they always were.

## Two modes

| Mode | Token on the wire | Gives you | Reach for it when |
|------|-------------------|-----------|-------------------|
| **Signed** — `CursorTokenSigner` | `<payload>.<hmac>` | tamper-evidence (HMAC-SHA256, constant-time verify) | you need integrity only; the payload staying readable is fine |
| **Encrypted** — `CursorTokenCipher` | `~<nonce‖ciphertext>` | confidentiality **and** integrity (AEAD) | boundary sort values must stay hidden, or you don't want cursor internals introspected |

A signed token appends an HMAC of the encoded payload; the payload itself stays
readable base64-JSON. An encrypted token is prefixed `~` and hides the whole
payload behind AES-256-GCM (swap the `aead` for ChaCha20-Poly1305 on hosts
without AES-NI). The AEAD tag already authenticates, so a cipher **supersedes**
a signer — configure both and the cipher wins; there is no signed-and-encrypted
stack.

Both take a `secret` of at least 32 bytes. The cipher derives its AES-256 key
from that secret by domain-separated hashing — it is a static-secret cipher,
deliberately not KMS-backed: a cursor is short-lived query state, not data at
rest, so it doesn't earn a key-management round-trip. The flip side: rotating
the secret invalidates every in-flight cursor (see
[the cutover](#enabling-is-a-hard-cutover)).

## Turning it on

Set the signer (or cipher) on the `ExecutionRuntime`; it binds per scope, so
every keyset mint and verify inside that runtime uses it — across the document,
search, and hub-search paths at once, with no per-backend wiring:

```python
from forze.application.contracts.querying import CursorTokenSigner
from forze.application.execution import ExecutionRuntime

runtime = ExecutionRuntime(
    deps=deps.freeze(),
    lifecycle=lifecycle.freeze(),
    cursor_token_signer=CursorTokenSigner(secret=load_secret("cursor-hmac")),
)
```

For confidentiality, pass `cursor_token_cipher=CursorTokenCipher(secret=...)`
instead. The binding is context-scoped: two runtimes in one process each mint
and verify with their own key rather than clobbering a shared global. Outside a
runtime — a script, a custom host — `configure_cursor_signer` /
`configure_cursor_cipher` set it for the current context at startup (like
`configure_logging`), and `bind_cursor_signer` / `bind_cursor_cipher` are the
scoped, auto-restoring variants.

The everywhere-at-once semantics is the point: because every mint and verify
falls back to the one bound signer, there is no way to end up minting signed
tokens on one path and accepting unsigned ones on another from a missed call
site.

## What the token is bound to

Under a signer or cipher, each token also embeds an authenticated digest of the
query context it was minted against:

- the **spec** name (the search or query spec, when the path has one),
- the **tenant** bound at mint time,
- a deterministic **fingerprint of the filter** (canonicalized, so the same
  filter fingerprints identically across pages and processes).

Verification recomputes the digest from the *current* request and rejects a
mismatch — so a validly-signed cursor cannot be replayed against a different
spec, another tenant, or a changed filter. The **sort** is not part of the
digest because it is already checked structurally: a cursor whose keys,
directions, or null placement don't match the active sort is rejected on every
path, signed or not.

Without a signer or cipher the binding is not embedded at all — unauthenticated,
a client could recompute it, so it would be integrity theater. That is also what
keeps the unprotected token byte-identical to what it was before the feature
existed.

## Enabling is a hard cutover

Verification under a signer rejects **any unsigned token**, and under a cipher
**any unencrypted one**. There is no accept-both grace mode: a grace window
would let a forged unsigned cursor through for exactly as long as it lasted,
which is the attack the feature exists to stop.

So when you enable protection (or rotate the cipher secret), cursors minted
before the flip fail their next use with the framework's `validation` error —
`core.validation`, HTTP 422 — and the client restarts pagination from page one.
That is the whole blast radius: cursors are ephemeral by nature, nothing stored
is affected, and every token minted after the flip carries protection.

!!! note "Tampered means invalid, nothing more"

    Every rejection — bad HMAC, failed AEAD open, binding mismatch, a forged
    value of the wrong type — surfaces as the same uniform *invalid cursor
    token* validation error. The response never distinguishes *why* the token
    failed, so a probing client learns nothing about the crypto layer or the
    query it tried to replay against.

Signed cursors keep integrity problems out of your result sets; what the rows
themselves expose is governed by [encryption](encryption.md) — and how a cursor
walk behaves under writes is part of
[reading data](../data-events/reading-data.md).
