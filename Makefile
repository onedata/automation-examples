# automation-examples -- root tooling (lambda v3). A uv workspace of example lambdas:
# dev tasks run in the workspace .venv via `uv run`; images via plain `docker build`.

# Image registry: dev by default; append REGISTRY=onedata for the public image.
DEV_REGISTRY    := docker.onedata.org
PUBLIC_REGISTRY := onedata
REGISTRY        ?= $(DEV_REGISTRY)

# Local SDK source, vendored as a wheel until it's published to PyPI (see `vendor-sdk`).
SDK_REPO ?= ../onedata-lambda-utils

# Migrated lambdas (= directories with a pyproject in the lambdas/ workspace).
LAMBDAS := $(notdir $(patsubst %/,%,$(dir $(wildcard lambdas/*/pyproject.toml))))

# Image for a lambda: <registry>/lambda-<name>:v<version> (version from its pyproject).
lambda_version = $(shell python3 -c "import tomllib; print(tomllib.load(open('lambdas/$(1)/pyproject.toml','rb'))['project']['version'])")
lambda_image   = $(REGISTRY)/lambda-$(1):v$(call lambda_version,$(1))

bold := $(shell tput bold)
normal := $(shell tput sgr0)
blue := $(shell tput setaf 4)

define print_target
	@echo ""
	@echo "$(blue)$(bold)$@:$(normal)"
endef

.DEFAULT_GOAL := help
.PHONY: help sync format format-check static-analysis type-check test lint check \
        build build-all publish publish-all image-name image-names clean vendor-sdk \
        _require_lambda _require_lambda_selector \
        check-lambda-image-matches-registry \
        check-workflow-images-public check-workflow-images-published \
        check-lambda-images-used

# `make help` groups targets by `##@ section` banners and lists each `target: ## description`.
help:
	@echo "Usage: make <target> [LAMBDA=<name>] [REGISTRY=$(PUBLIC_REGISTRY)] [YES=1]"
	@awk 'BEGIN{FS=":.*## "} /^##@ /{printf "\n%s:\n",substr($$0,5)} /^[a-z][a-zA-Z0-9_-]*:.*## /{printf "  %-20s %s\n",$$1,$$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "lambdas:"
	@echo "$(LAMBDAS)" | fmt -w 76 | sed 's/^/  /'

##@ dev

sync: ## sync the dev env (uv sync --all-packages)
	uv sync --all-packages

format: ## ruff format + autofix
	$(call print_target)
	uv run ruff format .
	uv run ruff check --fix .

format-check: ## ruff format --check
	$(call print_target)
	uv run ruff format --check .

static-analysis: ## ruff check
	$(call print_target)
	uv run ruff check .

type-check: ## mypy (src only)
	$(call print_target)
	uv run mypy lambdas/*/src packages/*/src

lint: format-check static-analysis type-check ## format-check + static-analysis + type-check
	@:

test: ## pytest (+ junit for CI)
	$(call print_target)
	uv run pytest --junitxml=automation-examples-tests-results.xml

check: lint test ## lint + test

##@ images

build: _require_lambda ## build one image (LAMBDA=<name>)
	docker build --build-arg LAMBDA_PACKAGE=$(LAMBDA) -t $(call lambda_image,$(LAMBDA)) .

publish: _require_lambda ## push one image (LAMBDA=<name>, optional YES=1)
	python3 utils/manage_lambdas.py confirm-publish --registry "$(REGISTRY)" "$(LAMBDA)" $(if $(filter 1,$(YES)),--yes,)
	docker push $(call lambda_image,$(LAMBDA))

build-all: ## build every lambda
	@set -e; for l in $(LAMBDAS); do echo ">> build $$l"; $(MAKE) --no-print-directory build LAMBDA=$$l; done

publish-all: ## push every lambda (optional YES=1)
	python3 utils/manage_lambdas.py confirm-publish --registry "$(REGISTRY)" all $(if $(filter 1,$(YES)),--yes,)
	@set -e; $(foreach lambda,$(LAMBDAS),echo ">> publish $(call lambda_image,$(lambda))"; docker push "$(call lambda_image,$(lambda))";)

image-name: _require_lambda ## print the resolved image:tag (LAMBDA=<name>)
	@echo $(call lambda_image,$(LAMBDA))

image-names: ## print all resolved images and tags
	@$(foreach lambda,$(LAMBDAS),echo $(call lambda_image,$(lambda));)

##@ validation

check-lambda-image-matches-registry: _require_lambda_selector ## compare local and published images (LAMBDA=<name>|all)
	python3 utils/manage_lambdas.py check-image-matches-registry --registry "$(REGISTRY)" "$(LAMBDA)"

check-workflow-images-public: ## check that lambda images used in workflows use the public onedata registry
	python3 utils/manage_workflows.py check-public "$(LAMBDA)"

check-workflow-images-published: ## check that lambda images used in workflows are published
	python3 utils/manage_workflows.py check-published "$(LAMBDA)"

check-lambda-images-used: ## check that every current lambda image is used in at least one workflow
	python3 utils/manage_workflows.py check-lambda-images-used "$(LAMBDA)"

##@ housekeeping

clean: ## remove tool caches + __pycache__
	rm -rf .ruff_cache .mypy_cache .pytest_cache
	@find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true

vendor-sdk: ## rebuild & vendor the local SDK wheel (temporary, pre-PyPI)
	cd $(SDK_REPO) && uv build --wheel --no-create-gitignore --out-dir "$(CURDIR)/vendor"
	uv lock --refresh-package onedata-lambda-utils

_require_lambda:
	@test -n "$(LAMBDA)" && test -d "lambdas/$(LAMBDA)" || { echo "error: set LAMBDA to one of: $(LAMBDAS)"; exit 1; }

_require_lambda_selector:
	@test -n "$(strip $(LAMBDA))" || { echo "error: set LAMBDA=<name> or LAMBDA=all"; exit 1; }
	@test "$(strip $(LAMBDA))" = "all" || test -d "lambdas/$(strip $(LAMBDA))" || { echo "error: set LAMBDA to one of: $(LAMBDAS), or use LAMBDA=all"; exit 1; }
