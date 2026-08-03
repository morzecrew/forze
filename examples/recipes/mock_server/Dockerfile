# A frontend team's mock backend: your app, in-memory, one container.
#
# Nothing here is production packaging — there is no database, no migration step and no
# secret handling, because the whole image is a development fixture. It exists so a
# frontend or CI job can `docker run` the API without a Python toolchain.
FROM python:3.13-slim

# uv resolves and installs far faster than pip on a cold layer, which matters for an image
# that is rebuilt whenever the app changes.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

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
