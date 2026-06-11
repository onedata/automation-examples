# syntax=docker/dockerfile:1.7
#
# Canonical Dockerfile for an Onedata Automation lambda built on lambda-base v3.
# Copy this into your lambda repo and do NOT edit the build section --
# all build logic lives in `build-lambda` inside the base image, so it
# can evolve via a base-image bump without touching your repo.
#
# Build:
#   single-lambda repo:    docker build -t my-lambda:dev .
#   uv workspace member:   docker build --build-arg LAMBDA_PACKAGE=<member> -t <member>:dev .
#
# Requires Docker >= 23 (BuildKit) for --mount=type=bind / --mount=type=cache.

FROM onedata/lambda-base-slim:v3

# Empty => single-lambda repo (the build context root is the package).
# Set to a uv workspace member name to build just that lambda's dependency subtree.
ARG LAMBDA_PACKAGE=

# build-lambda runs as root (apt for declared [tool.onedata.lambda] system-packages),
# then drops to the unprivileged `app` user via gosu for `uv sync` so the resulting
# venv is app-owned. USER app restores the non-root runtime user for the container.
USER root
RUN --mount=type=cache,target=/home/app/.cache/uv \
    --mount=type=bind,source=.,target=/build \
    build-lambda "$LAMBDA_PACKAGE"
USER app
