# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Domain document support in `forze.domain` built from `forze.domain.models.Document` with name/number/soft-deletion mixins and update-validator infrastructure for safer incremental updates.
- Document kernel in `forze.application.kernel`: pluggable usecase plans, `DocumentUsecasesFacade` factory, `DocumentPort` with explicit `DocumentSearchPort` and `DocumentReadPort`/`DocumentWritePort`, and `DocumentOperation` enum for operation keys.
- Optional FastAPI integration package `forze_fastapi` (extra `fastapi`): routing helpers, idempotent POST support, and prebuilt document router.
- Optional provider packages (separate installs): `forze_postgres`, `forze_redis`, `forze_s3`, `forze_temporal`, `forze_mongo` with platform clients, gateways/adapters, and dependency keys for composition.

### Changed

- **Postgres filter builder** (in `forze_postgres.kernel.builder`): filter input accepts only canonical operator names (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`, `or`, plus array and ltree ops). Aliases such as `==`, `ge`, `not in`, `in_`, `or_` are no longer accepted and raise `ValidationError`. Use `in` and `or` for membership and disjunction.
- Infrastructure previously under `forze.infra` has been moved into optional packages; core `forze` no longer ships Postgres, Redis, S3, or Temporal implementations.

### Fixed

- Correct UUIDv7 datetime conversion in `forze.base.primitives.uuid` so round-trips between datetimes and UUIDs preserve timestamp semantics.

## [0.1.1] - 2026-02-23

### Added

- Initial DDD/Hex contracts: ports, results, errors.

### Fixed

- Packaging metadata for PyOCI classifiers.

[unreleased]: https://github.com/morzecrew/forze/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/morzecrew/forze/releases/tag/v0.1.1
