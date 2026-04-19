# syntax=docker/dockerfile:1.7
#
# Moraine dev sandbox image — NOT published, NOT a distribution channel.
# See RFC #232 for background. Only used by scripts/dev/sandbox/moraine-sandbox.
#
# Single-stage image built on rust:1-bookworm so agents inside a running
# sandbox can iterate on the workspace — cargo build / test / clippy all just
# work, wrapped by sccache sharing the host's cache dir.
#
# Boot flow:
#   1. host CLI bind-mounts the worktree at /repo (ro).
#   2. entrypoint.sh runs `cargo build --workspace --locked` from /repo into
#      a per-sandbox CARGO_TARGET_DIR volume on first boot (or on --rebuild).
#   3. installs the freshly-built moraine / moraine-ingest / moraine-monitor
#      / moraine-mcp into the /opt/moraine/bin named volume.
#   4. runs `moraine up` against the sibling-compose ClickHouse service.
#
# Everything that varies per sandbox — worktree contents, cargo target dir,
# cargo home, config, entrypoint.sh — arrives via bind mount or named volume,
# so this image is built once and shared across every sandbox on the host.
#
# Size: ~1.5 GB (rust toolchain dominates). Paid once; cached for every up.

FROM rust:1-bookworm

# sccache version — kept in sync with whatever the host has installed so disk
# cache entries are wire-compatible. Update both host (via `cargo install
# sccache`) and this value together.
ARG SCCACHE_VERSION=0.14.0
ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PATH=/opt/moraine/bin:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    MORAINE_MONITOR_STATIC_DIR=/opt/moraine/web \
    RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/home/moraine/.cache/sccache \
    SCCACHE_CACHE_SIZE=20G \
    CARGO_HOME=/home/moraine/.cargo \
    CARGO_TARGET_DIR=/home/moraine/target

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
        xz-utils \
        tzdata \
        procps \
        pkg-config \
        libssl-dev \
        cmake \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js (for the Claude Code CLI used by the agent-smoke-e2e driver).
# NodeSource's setup script is the upstream-recommended path and pins us to a
# current LTS line. We install npm with it so `npm install -g @anthropic-ai/claude-code`
# just works for the smoke-test flow; non-smoke sandbox users pay ~50 MB once,
# which is cheap compared to the rust toolchain that dominates image size.
ARG NODE_MAJOR=20
RUN curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @anthropic-ai/claude-code \
    && claude --version

# Install sccache. Binary targets linux-musl for static linkage so it runs
# regardless of glibc version. When the host (macOS) and the container (linux)
# both point RUSTC_WRAPPER=sccache at a shared disk cache, their entries are
# content-addressed by {compiler, flags, source} and so live side-by-side
# without collision — darwin and linux outputs share the 20G budget but never
# confuse each other.
RUN case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
        arm64)  sccache_triple=aarch64-unknown-linux-musl ;; \
        amd64)  sccache_triple=x86_64-unknown-linux-musl  ;; \
        *)      echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${sccache_triple}.tar.gz" \
        | tar -xz --strip-components=1 -C /usr/local/bin "sccache-v${SCCACHE_VERSION}-${sccache_triple}/sccache" \
    && chmod 0755 /usr/local/bin/sccache \
    && /usr/local/bin/sccache --version

# Non-root user matching the host bind-mount ownership contract. The rust
# toolchain under /usr/local/{cargo,rustup} is world-readable so moraine can
# invoke cargo/rustc/rustup without modification.
RUN groupadd --system --gid 1000 moraine \
    && useradd --system --uid 1000 --gid 1000 \
        --home-dir /home/moraine --create-home --shell /bin/bash \
        moraine

# Runtime filesystem layout. Named volumes land on /opt/moraine/bin (built
# binaries), /home/moraine/.cargo (registry), /home/moraine/target (cargo
# target dir). Bind mounts land on /opt/moraine/web, /sandbox, /repo, and
# /home/moraine/.cache/sccache. Pre-create with correct ownership so empty
# volumes inherit moraine:moraine rather than root:root.
RUN mkdir -p \
        /opt/moraine/bin \
        /opt/moraine/web \
        /sandbox \
        /home/moraine/.moraine \
        /home/moraine/.cache/sccache \
        /home/moraine/.cargo \
        /home/moraine/target \
    && chown -R moraine:moraine \
        /opt/moraine \
        /sandbox \
        /home/moraine

# entrypoint.sh is bind-mounted by compose at /usr/local/bin/entrypoint.sh from
# the active worktree, so it always tracks the current scripts/dev/sandbox/
# version. The placeholder here ensures the file exists (and is executable) if
# someone runs the image without compose's bind mount, and also forces a rebuild
# when the contract changes.
RUN install -m 0755 /dev/null /usr/local/bin/entrypoint.sh \
    && printf '#!/bin/sh\necho "entrypoint.sh bind-mount missing — start with moraine-sandbox, not bare docker run" >&2\nexit 1\n' \
        > /usr/local/bin/entrypoint.sh \
    && chmod 0755 /usr/local/bin/entrypoint.sh

USER moraine
WORKDIR /home/moraine

# tini reaps zombies and forwards signals to entrypoint.sh.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD []
