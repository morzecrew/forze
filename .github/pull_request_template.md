<!--
Title: <gitmoji> <type>[scope][!]: <description>   (gitmoji-conventional)
e.g.  ✨ feat(ports): add outbox port for transactional dispatch
-->

## Summary

<!-- What changed and why, in 2–4 sentences. Not a file list. -->

Closes #

## Type

- [ ] Feature
- [ ] Fix
- [ ] Refactor (no behaviour change)
- [ ] Docs
- [ ] Chore / CI / dependencies
- [ ] Breaking change

## Architecture

- [ ] `domain/` imports nothing from `application/` or `infrastructure/`
- [ ] `application/` depends on ports, never on a concrete adapter
- [ ] Core stays framework-agnostic — no FastAPI / SQLAlchemy / socketio import
      reachable from a core module
- [ ] Any new optional-dependency import is lazy or guarded; `import forze`
      still works with zero extras installed

## Public API

- [ ] No change to the public surface
- [ ] Additive only
- [ ] Breaking — migration path below and in `CHANGELOG.md`

Deprecations, renames, or changed signatures:

## Verification

- [ ] `just ci` green locally
- [ ] New behaviour has a test that fails without the change (seen red first)
- [ ] Domain-layer tests need no database, broker, or HTTP client

## Downstream & docs

- [ ] `skills/` updated — this changes wiring, handler, or integration patterns
      that consuming services follow
- [ ] `skills/` untouched — nothing a consumer-facing skill describes has moved
- [ ] `pages/` updated
- [ ] `CHANGELOG.md` `## [Unreleased]` updated in Keep a Changelog categories

## Risk

<!-- Blast radius for a service already depending on Forze. Anything that only
     shows up at runtime, under a specific extra, or on upgrade. -->