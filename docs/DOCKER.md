# Using Forze in Docker

When building Docker images that depend on `forze`, the build must authenticate against the private registry via PyOCI. Credentials must **not** be stored in Docker image layers. This document explains the correct and secure approach.

## The Problem

Docker image layers are immutable. If credentials are passed via `ARG`, `ENV`, or copied files, they can end up in image history or build cache. Credentials must exist only during the dependency installation step and never be persisted in the image.

## Solution: Docker BuildKit Secrets

Docker BuildKit allows mounting secrets that:

- exist only during a specific `RUN` step
- are never committed to image layers
- do not appear in `docker history`

This is the recommended and secure approach.

## Build Flow

1. Provide a GitHub token at build time
2. Pass it as a BuildKit secret
3. Mount the secret in the Dockerfile
4. Export the required environment variables in the same `RUN` step
5. Install dependencies
6. Continue the build as usual

The token never becomes part of the final image. Example Dockerfile:

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.13-slim AS builder

WORKDIR /app

# Copy dependency definitions first (for caching)
COPY pyproject.toml uv.lock ./

# Install dependencies securely
RUN --mount=type=secret,id=pyoci_token \
    set -euo pipefail; \
    export UV_INDEX_PYOCI_USERNAME="github"; \
    export UV_INDEX_PYOCI_PASSWORD="$(cat /run/secrets/pyoci_token)"; \
    uv sync --frozen

# Copy the rest of the application code
COPY . .

# Continue with build steps...
```

### Local Build

Export your token, then build with BuildKit:

```bash
export UV_INDEX_PYOCI_PASSWORD=$(gh auth token)

docker buildx build \
    --secret id=pyoci_token,env=UV_INDEX_PYOCI_PASSWORD \
    -t my-app:latest .
```

The secret exists only during the corresponding `RUN` step; it is not stored in layers and does not appear in image history.

### CI Build

GitHub Actions example:

```yaml
- name: Build Image
  uses: docker/build-push-action@v7
  with:
    context: .
    push: true
    tags: my-app:latest
    secrets: |
      pyoci_token=${{ secrets.GITHUB_TOKEN }}
```

The same Dockerfile works for both local and CI builds.