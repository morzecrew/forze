# Spec registry

The **spec inventory**: one declaration of every logical spec an application binds, so
the framework can answer questions about the app as a whole rather than one route at a
time. Three features already read it — portability, quiesce, and the routeless-provider
guard — and each of them needs the same thing: the list of planes this app actually has.

```python
from forze.application.contracts.inventory import SpecRegistry

specs = (
    SpecRegistry()
    .register(OrderSpec, OrderSearchSpec, InvoiceBlobSpec)
    .freeze()
)
```

Pass it to `assemble(...)` as `spec_registry=specs` and it becomes part of the runtime.

## Why an inventory exists at all

A `DepsRegistry` answers "what provides this route?". Nothing answered "what routes does
this application have?" — and that question has no answer you can derive, because a spec
is only visible at the moment a handler resolves it. Three consequences followed, and the
inventory is the shared fix rather than three local ones:

- **Portability** cannot export what it cannot enumerate. An export walks the inventory,
  not the deps registry, because a provider bound to a route says nothing about the shape
  the route stores.
- **Quiesce** must drain every plane; a plane it does not know about stays live while the
  report says the system is quiet.
- **A routeless provider** — a plane whose provider is registered but whose route was
  never catalogued — used to resolve happily and silently serve the wrong thing.

## Keyed by `(plane, name)`, never by the spec

Entries are keyed by plane and name. Two reasons, both of which bite:

`DocumentSpec` and `SearchSpec` are **unhashable** (their `write` mapping is a dict and
`fields` a list), so a `set` of specs raises `TypeError` rather than quietly failing to
dedupe. And a route is always a plain string while a spec's `name` may be a `StrEnum` —
the two must compare, so the name is coerced on the way in.

## Building one

| Method | Does |
|---|---|
| `register(*specs, disposition=None)` | catalogue specs, inferring their plane |
| `register_entry(entry)` | catalogue a pre-built `SpecRegistryEntry` (plane, name, disposition, source) |
| `link(kind, *, source, target)` | record an edge between two specs — a document and the search index that mirrors it |
| `merge(*others)` | fold in a kit's or a module's own registry |
| `freeze()` | produce the immutable `FrozenSpecRegistry` the runtime holds |

A frozen registry is the read surface: `of_plane`, `find`, `of_disposition`, `edges_of`,
plus `spec_fingerprint` and `fingerprint` — a shape digest an import target can compare
against the archive it was handed, so a mismatch fails before it writes anything.

## Disposition: what an export may do with a spec

Each entry carries a `PlaneDisposition`. It is the spec's own statement about portability,
not a flag the export passes: a cache is *derived* and must never be restored from an
archive, a mailbox is *transient*, a document collection is *portable*.

Entries also carry `identity`, which is provenance rather than shape — it changes *which*
export carries the spec, not what a target must be able to import. A per-tenant export
excludes identity specs by default (a data-portability request wants the tenant's business
data, not their session tokens); a full-system export always carries them, because a live
system needs its sessions.

## The route guard

Declaring a `spec_registry` installs a resolve-time guard: an uncatalogued route on an
inventoried plane is refused at first use, whatever the provider's shape. That is the
routeless-provider blind spot closed — but it also means adding a plane and forgetting to
register its spec now fails loudly instead of serving. `allow_unregistered=True` downgrades
it to one warning per route while you migrate.

## See also

- [Portability](../running-in-prod/portability.md) — export, import and migrate, all of
  which walk the inventory.
- [Shutdown and fleets](../running-in-prod/shutdown-and-fleets.md) — quiesce drains the
  planes the inventory names.
- [Wiring](../writing-operation/wiring.md) — where `spec_registry` is passed to `assemble`.
