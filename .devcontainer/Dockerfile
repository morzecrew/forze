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

RUN curl -LsSf https://astral.sh/uv/0.10.9/install.sh | bash && \
    curl -LsSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin --tag 1.46.0 && \
    curl -LsSf https://d2lang.com/install.sh | VERSION=v0.7.1 bash -s --

USER vscode
WORKDIR /workspaces/forze
