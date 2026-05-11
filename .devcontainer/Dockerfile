FROM mcr.microsoft.com/devcontainers/python@sha256:9e51c13505754efe79dd4a0b1a29523d3854090dabb8add3f3fa642ea3c98fad

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV UV_INSTALL_DIR=/usr/local/bin \
    PATH="/usr/local/bin:${PATH}"

RUN apt-get update  \
    && apt-get install -y --no-install-recommends \
    graphviz=2.42.4-3 \
    curl=8.14.1-2+deb13u2 \
    git=1:2.47.3-0+deb13u1 \
    shellcheck=0.10.0-1 \
    && rm -rf /var/lib/apt/lists/*

ARG JUST_VERSION=1.46.0
ARG D2_VERSION=v0.7.1
ARG UV_VERSION=0.10.9

RUN set -eux; \
    curl -LsSf -o /tmp/just.tar.gz \
    "https://github.com/casey/just/releases/download/${JUST_VERSION}/just-${JUST_VERSION}-x86_64-unknown-linux-musl.tar.gz"; \
    tar -xzf /tmp/just.tar.gz -C /tmp; \
    mv /tmp/just /usr/local/bin/just; \
    chmod +x /usr/local/bin/just; \
    \
    curl -LsSf -o /tmp/d2.tar.gz \
    "https://github.com/terrastruct/d2/releases/download/${D2_VERSION}/d2-${D2_VERSION}-linux-amd64.tar.gz"; \
    mkdir -p /tmp/d2-extract; \
    tar -xzf /tmp/d2.tar.gz -C /tmp/d2-extract; \
    mv "$(find /tmp/d2-extract -type f -name d2 | head -n 1)" /usr/local/bin/d2; \
    chmod +x /usr/local/bin/d2; \
    \
    curl -LsSf -o /tmp/uv.tar.gz \
    "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz"; \
    mkdir -p /tmp/uv-extract; \
    tar -xzf /tmp/uv.tar.gz -C /tmp/uv-extract; \
    mv "$(find /tmp/uv-extract -type f -name uv | head -n 1)" /usr/local/bin/uv; \
    mv "$(find /tmp/uv-extract -type f -name uvx | head -n 1)" /usr/local/bin/uvx; \
    chmod +x /usr/local/bin/uv /usr/local/bin/uvx; \
    \
    rm -rf /tmp/*.tar.gz /tmp/*.tgz /tmp/d2-extract /tmp/uv-extract

USER vscode
WORKDIR /workspaces/forze
