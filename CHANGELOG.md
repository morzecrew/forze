# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Infra layer (`forze.infra`) with Postgres, Redis, S3, and Temporal providers (gateways, platform clients, shared errors and codecs).
- Domain document support built from `forze.domain.models.Document` with reusable name/number/soft-deletion mixins and update-validator infrastructure for safer incremental updates.
- Optional FastAPI integration package (`forze_fastapi`) with routing helpers and a `fastapi` extra.

### Changed

- **Postgres filter builder** (`forze.infra.providers.postgres.builder`): filter input now accepts only canonical operator names (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`, `or`, array and ltree ops). Aliases such as `==`, `ge`, `not in`, `in_`, `or_` are no longer accepted and raise `ValidationError`. Use `in` and `or` (not `in_`/`or_`) for membership and disjunction.
- Document kernel registry and composition updated to support pluggable plans and a `DocumentUsecasesFacade` factory for document operations.
- Document search ports unified under `DocumentPort` with explicit `DocumentSearchPort` support for search usecases.

### Fixed

- Correct UUIDv7 datetime conversion in `forze.base.primitives.uuid` so round-trips between datetimes and UUIDs preserve timestamp semantics.

## [0.1.1] - 2026-02-23

### Added

- Initial DDD/Hex contracts: ports, results, errors.

### Fixed

- Packaging metadata for PyOCI classifiers.

[unreleased]: https://github.com/morzecrew/forze/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/morzecrew/forze/releases/tag/v0.1.1
