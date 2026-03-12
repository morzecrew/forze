FROM mcr.microsoft.com/devcontainers/python@sha256:9b0762901a555c31c2c63824cb5b1ade77da939500e034317b8d09578b94ba0e

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update  \
    && apt-get install -y --no-install-recommends \
    graphviz=2.42.4-3 \
    curl=8.14.1-2+deb13u2 \
    git=1:2.47.3-0+deb13u1 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/0.10.9/install.sh | sh && \
    curl -LsSf https://just.systems/install.sh | sh -s -- --to /usr/local/bin --tag 1.46.0 && \
    curl -fsSL https://d2lang.com/install.sh | VERSION=v0.7.1 sh -s --

ENV PATH="/home/vscode/.local/bin:${PATH}"

USER vscode
WORKDIR /workspaces/forze