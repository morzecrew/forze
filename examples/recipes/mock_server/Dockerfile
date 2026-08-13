# A frontend team's mock backend: your app, in-memory, one container.
#
# Nothing here is production packaging — there is no database, no migration step and no
# secret handling, because the whole image is a development fixture. It exists so a
# frontend or CI job can `docker run` the API without a Python toolchain.
#
# Both images below are pinned by digest, as `.devcontainer/Dockerfile` already is: a tag is
# a moving pointer, so an unpinned build is not reproducible and silently adopts whatever the
# tag points at today. To refresh either one, resolve the current index digest and record
# what it turned out to be:
#
#     docker buildx imagetools inspect python:3.13-slim --format '{{.Manifest.Digest}}'
#
# python:3.13-slim → Python 3.13.15
FROM python:3.13-slim@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a

# uv resolves and installs far faster than pip on a cold layer, which matters for an image
# that is rebuilt whenever the app changes.
#
# ghcr.io/astral-sh/uv:latest → uv 0.12.3
COPY --from=ghcr.io/astral-sh/uv@sha256:2d890623d310b57771ce840f0da5eed5fc6d657da05ffaa45d82797b53fa3abc /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies first, so editing the app does not re-resolve them.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --extra cli --extra mock-server --extra fastapi

COPY . .
RUN uv sync --frozen --extra cli --extra mock-server --extra fastapi

# The refusal gate, satisfied deliberately and visibly: this image *is* the mock, and the
# declaration says so in its own name. Anything running this container is opting in.
ENV FORZE_MOCK_SERVER=1
ENV MOCK_APP=examples.recipes.mock_server.served:mock_app

EXPOSE 8000

# 0.0.0.0 because the port is published out of the container; the gate above, not the bind
# address, is what keeps this off a deployment.
CMD ["sh", "-c", "uv run forze mock serve \"$MOCK_APP\" --host 0.0.0.0 --port 8000"]
