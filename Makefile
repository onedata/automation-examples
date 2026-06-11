# automation-examples -- root tooling (lambda v3). A uv workspace of example lambdas:
# dev tasks run in the workspace .venv via `uv run`; images via plain `docker build`.

# Image registry: dev by default; append REGISTRY=onedata for the public image.
DEV_REGISTRY    := docker.onedata.org
PUBLIC_REGISTRY := onedata
REGISTRY        ?= $(DEV_REGISTRY)

# Local SDK source, vendored as a wheel until it's published to PyPI (see `vendor-sdk`).
SDK_REPO ?= ../onedata-lambda-utils

# Migrated lambdas (= the lambdas/ workspace members).
LAMBDAS := $(notdir $(wildcard lambdas/*))

# Image for a lambda: <registry>/lambda-<name>:v<version> (version from its pyproject).
lambda_version = $(shell python3 -c "import tomllib; print(tomllib.load(open('lambdas/$(1)/pyproject.toml','rb'))['project']['version'])")
lambda_image   = $(REGISTRY)/lambda-$(1):v$(call lambda_version,$(1))

.DEFAULT_GOAL := help
.PHONY: help sync format lint type-check test check \
        build build-all publish publish-all image-name clean vendor-sdk _require_lambda

# `make help` groups targets by `##@ section` banners and lists each `target: ## description`.
help:
	@echo "Usage: make <target> [LAMBDA=<name>] [REGISTRY=$(PUBLIC_REGISTRY)]"
	@awk 'BEGIN{FS=":.*## "} /^##@ /{printf "\n%s:\n",substr($$0,5)} /^[a-z][a-zA-Z0-9_-]*:.*## /{printf "  %-12s %s\n",$$1,$$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "lambdas:"
	@echo "$(LAMBDAS)" | fmt -w 76 | sed 's/^/  /'

##@ dev

sync: ## sync the dev env (uv sync --all-packages)
	uv sync --all-packages

format: ## ruff format + autofix
	uv run ruff format .
	uv run ruff check --fix .

lint: ## ruff format --check + check
	uv run ruff format --check .
	uv run ruff check .

type-check: ## mypy (src only)
	uv run mypy lambdas/*/src packages/*/src

test: ## pytest
	uv run pytest

check: lint type-check test ## lint + type-check + test

##@ images

build: _require_lambda ## build one image (LAMBDA=<name>)
	docker build --build-arg LAMBDA_PACKAGE=$(LAMBDA) -t $(call lambda_image,$(LAMBDA)) .

publish: _require_lambda ## push one image (LAMBDA=<name>)
	docker push $(call lambda_image,$(LAMBDA))

build-all: ## build every lambda
	@set -e; for l in $(LAMBDAS); do echo ">> build $$l"; $(MAKE) --no-print-directory build LAMBDA=$$l; done

publish-all: ## push every lambda
	@set -e; for l in $(LAMBDAS); do echo ">> publish $$l"; $(MAKE) --no-print-directory publish LAMBDA=$$l; done

image-name: _require_lambda ## print the resolved image:tag (LAMBDA=<name>)
	@echo $(call lambda_image,$(LAMBDA))

##@ housekeeping

clean: ## remove tool caches + __pycache__
	rm -rf .ruff_cache .mypy_cache .pytest_cache
	@find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true

vendor-sdk: ## rebuild & vendor the local SDK wheel (temporary, pre-PyPI)
	cd $(SDK_REPO) && uv build --wheel --out-dir "$(CURDIR)/vendor"
	uv lock --refresh-package onedata-lambda-utils

_require_lambda:
	@test -n "$(LAMBDA)" && test -d "lambdas/$(LAMBDA)" || { echo "error: set LAMBDA to one of: $(LAMBDAS)"; exit 1; }
