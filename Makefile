SHELL := /bin/bash
UV ?= uv
PYTHON ?= python3
CODEX ?= codex
AGENT_PLUGINS_SOURCE ?= main
AGENT_PLUGINS_REMOTE ?= origin
DOCS_ADDR ?= 127.0.0.1:8000
RUSTUP ?= rustup
RUST_TOOLCHAIN ?= stable
RUSTUP_BIN_DIR ?= $(shell command -v $(RUSTUP) >/dev/null 2>&1 && dirname "$$(command -v $(RUSTUP))" || printf '%s' "$$HOME/.cargo/bin")
USE_RUSTUP ?= 1
CARGO_CMD ?= cargo

ifeq ($(USE_RUSTUP),1)
CARGO_ENV = PATH='$(RUSTUP_BIN_DIR):$(PATH)' RUSTUP_TOOLCHAIN='$(RUST_TOOLCHAIN)'
DEFAULT_CARGO_TARGET_DIR = target/rustup-$(RUST_TOOLCHAIN)
else
CARGO_ENV =
DEFAULT_CARGO_TARGET_DIR = target/host
endif

CARGO_TARGET_DIR ?= $(DEFAULT_CARGO_TARGET_DIR)
RUST_LABEL = $(if $(filter 1,$(USE_RUSTUP)),$(RUST_TOOLCHAIN) via $(RUSTUP_BIN_DIR),host PATH)

.PHONY: help dependency-policy fmt clippy build test ci-check install docs-build docs-serve docs-clean sandbox-up sandbox-down sandbox-list hooks-install agent-plugins-install

help:
	@echo "Available targets:"
	@echo "  make dependency-policy"
	@echo "                       Enforce the epic #451 Phase 1 dependency rules"
	@echo "  make fmt            Check Rust formatting"
	@echo "  make clippy         Run strict clippy baseline"
	@echo "  make build          Build all workspace crates"
	@echo "  make test           Run workspace tests"
	@echo "  make ci-check       Run local CI checks: dependency policy, fmt, clippy, build, test"
	@echo "  make install        Build the current checkout and install it to the host"
	@echo "  make docs-build     Build static docs site into ./site"
	@echo "  make docs-serve     Run live docs server at $(DOCS_ADDR)"
	@echo "  make docs-clean     Remove generated docs site output"
	@echo "  make sandbox-up     Bring up a dev sandbox (see scripts/dev/sandbox/README.md)"
	@echo "  make sandbox-down   Tear down all dev sandboxes owned by this user"
	@echo "  make sandbox-list   List running dev sandboxes"
	@echo "  make hooks-install  Enable repo-managed git hooks (fmt + clippy pre-commit)"
	@echo "  make agent-plugins-install"
	@echo "                       Sync developer agent Codex plugins from $(AGENT_PLUGINS_REMOTE)/main"
	@echo ""
	@echo "Rust toolchain: $(RUST_LABEL)"
	@echo "Cargo command: $(CARGO_CMD)"
	@echo "Cargo target dir: $(CARGO_TARGET_DIR)"
	@echo "Override with: make ci-check USE_RUSTUP=0 CARGO_TARGET_DIR=target/nix"

dependency-policy:
	@echo "[dependency-policy] $(RUST_LABEL)"
	@$(PYTHON) -m unittest -v scripts/ci/test_dependency_policy.py
	@$(CARGO_ENV) MORAINE_CARGO='$(CARGO_CMD)' $(PYTHON) scripts/ci/dependency_policy.py

fmt:
	@echo "[fmt] $(RUST_LABEL)"
	@$(CARGO_ENV) $(CARGO_CMD) fmt --all -- --check

clippy:
	@echo "[clippy] $(RUST_LABEL); target $(CARGO_TARGET_DIR)"
	@$(CARGO_ENV) CARGO_TARGET_DIR='$(CARGO_TARGET_DIR)' MORAINE_CARGO='$(CARGO_CMD)' bash scripts/ci/clippy-strict-baseline.sh

build:
	@echo "[build] $(RUST_LABEL); target $(CARGO_TARGET_DIR)"
	@$(CARGO_ENV) CARGO_TARGET_DIR='$(CARGO_TARGET_DIR)' $(CARGO_CMD) build --workspace --locked

test:
	@echo "[test] $(RUST_LABEL); target $(CARGO_TARGET_DIR)"
	@$(CARGO_ENV) CARGO_TARGET_DIR='$(CARGO_TARGET_DIR)' $(CARGO_CMD) test --workspace --locked

ci-check: dependency-policy fmt clippy build test

# Build the current checkout for the host target and install it over the
# active `moraine` on your PATH. Pass flags via INSTALL_ARGS, e.g.
#   make install INSTALL_ARGS="--with-clickhouse"
install:
	scripts/dev/install-host.sh $(INSTALL_ARGS)

hooks-install:
	scripts/dev/install-hooks.sh

agent-plugins-install:
	CODEX_CMD='$(CODEX)' \
		AGENT_PLUGINS_SOURCE='$(AGENT_PLUGINS_SOURCE)' \
		AGENT_PLUGINS_REMOTE='$(AGENT_PLUGINS_REMOTE)' \
		scripts/dev/install-agent-plugins.sh

docs-build:
	$(UV) run --with zensical zensical build
	@echo "[docs] overlaying landing page onto site root"
	@cp web/landing/index.html site/index.html
	@cp web/landing/CNAME site/CNAME
	@mkdir -p site/assets
	@cp -R web/landing/assets/. site/assets/

docs-serve:
	$(UV) run --with zensical zensical serve --dev-addr $(DOCS_ADDR)

docs-clean:
	rm -rf site

sandbox-up:
	scripts/dev/sandbox/moraine-sandbox up $(SANDBOX_ARGS)

sandbox-down:
	scripts/dev/sandbox/moraine-sandbox down --all

sandbox-list:
	scripts/dev/sandbox/moraine-sandbox list
