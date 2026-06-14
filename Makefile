SHELL := /bin/bash
UV ?= uv
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

.PHONY: help fmt clippy build test ci-check docs-build docs-serve docs-clean sandbox-up sandbox-down sandbox-list hooks-install

help:
	@echo "Available targets:"
	@echo "  make fmt            Check Rust formatting"
	@echo "  make clippy         Run strict clippy baseline"
	@echo "  make build          Build all workspace crates"
	@echo "  make test           Run workspace tests"
	@echo "  make ci-check       Run local CI checks: fmt, clippy, build, test"
	@echo "  make docs-build     Build static docs site into ./site"
	@echo "  make docs-serve     Run live docs server at $(DOCS_ADDR)"
	@echo "  make docs-clean     Remove generated docs site output"
	@echo "  make sandbox-up     Bring up a dev sandbox (see scripts/dev/sandbox/README.md)"
	@echo "  make sandbox-down   Tear down all dev sandboxes owned by this user"
	@echo "  make sandbox-list   List running dev sandboxes"
	@echo "  make hooks-install  Enable repo-managed git hooks (fmt + clippy pre-commit)"
	@echo ""
	@echo "Rust toolchain: $(RUST_LABEL)"
	@echo "Cargo command: $(CARGO_CMD)"
	@echo "Cargo target dir: $(CARGO_TARGET_DIR)"
	@echo "Override with: make ci-check USE_RUSTUP=0 CARGO_TARGET_DIR=target/nix"

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

ci-check: fmt clippy build test

hooks-install:
	scripts/dev/install-hooks.sh

docs-build:
	$(UV) run --with zensical zensical build
	@echo "[docs] overlaying landing page onto site root"
	@cp web/landing/index.html site/index.html
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
