# Automation examples

Example automation lambdas and workflow schemas for [Onedata](https://onedata.org).

This repository serves two purposes:

1. **Examples** to get started writing your own lambdas and workflows.
2. **Ready-to-use workflow schema dumps** (`<name>.json`) you can load straight
   into an automation inventory — download the JSON and use the *Upload JSON*
   action in the workflows tab.

The lambdas are built on **lambda-base v3** and the
**[`onedata-lambda-utils`](https://pypi.org/project/onedata-lambda-utils/) SDK**.
Their full authoring documentation (the handler API, testing, streaming, file
access) lives with the SDK, under `docs/` in `onedata-lambda-utils`.

## Layout

This repo is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/):

```
automation-examples/
├── pyproject.toml        # virtual workspace root (members, shared dev tooling)
├── uv.lock               # one lockfile for every member
├── Dockerfile            # one canonical template, shared by all lambdas
├── lambdas/              # deployable lambdas — one workspace member each
│   └── <name>/
│       ├── pyproject.toml         # deps + the single onedata.lambda entry point
│       ├── src/<pkg>/handler.py   # the handler
│       └── <name>.json            # the workflow/lambda schema dump
├── packages/            # shared libraries used by several lambdas
└── utils/               # workflow/dump maintenance scripts
```

> `lambdas_old/` holds the legacy v2 lambdas, kept for reference during the v3
> migration — not part of the workspace.

For the full explanation of this layout, see `docs/guides/shared-code-uv-workspace.md`
in `onedata-lambda-utils`.

## Creating a lambda

Each lambda is a workspace member under `lambdas/<name>/`:

1. A `pyproject.toml` declaring its dependencies and **exactly one**
   `onedata.lambda` entry point:
   ```toml
   [project.entry-points."onedata.lambda"]
   handler = "<pkg>.handler:handle"
   ```
2. A handler in `src/<pkg>/handler.py` written against the SDK — a per-job
   `@per_job` function or a batch `handle(jobs, ctx)`. See
   `docs/guides/writing-a-handler.md` in `onedata-lambda-utils`.
3. A `<name>.json` schema dump (downloaded from the automation inventory GUI),
   used to register and run the lambda.

Shared logic goes in a `packages/*` member that lambdas depend on by name; the
contract `TypedDict`s stay in each lambda. The easiest start is to copy the
closest existing lambda and rework it — `echo` (minimal batch),
`calculate-checksum-mounted` (per-job, mounted file access), and
`calculate-checksum-rest` (per-job, REST) are good starting points.

## Building and publishing images

The canonical `Dockerfile` builds any member, selected by a build arg:

```bash
make build LAMBDA=calculate-checksum-mounted      # docker.onedata.org/lambda-<name>:v<version>
make publish LAMBDA=calculate-checksum-mounted    # push it
make build-all                                    # build every lambda
make publish-all REGISTRY=onedata                 # build/push public images (onedata/*)
```

`make build LAMBDA=<name>` is a thin wrapper over
`docker build --build-arg LAMBDA_PACKAGE=<name> .`; the image gets only that
lambda's dependency subtree. The tag's version comes from the member's
`pyproject.toml`. Run `make help` (or `make image-name LAMBDA=<name>`) for
details.

## Development

```bash
make sync     # uv sync --all-packages — one .venv with every member, editable
make check    # lint (ruff + mypy) + tests across the workspace
make test     # pytest only
```

> [!NOTE]
> The SDK is not yet on PyPI, so it is vendored as a wheel under `vendor/` and
> refreshed with `make vendor-sdk`. See
> `docs/guides/local-sdk-vendored-wheel.md` in `onedata-lambda-utils`.

## Testing guidelines

Each lambda should have focused tests under `lambdas/<name>/tests/`. Prefer
small, explicit fixtures and constants over repeated inline literals, especially
for file IDs, file names, metadata keys, REST URLs, checksum values, and expected
results.

Use `test_handler.py` for the lambda's own logic:

- call the handler directly with `build_jobs()` and `build_job_context()`;
- cover success paths, validation, error handling, and per-job exceptions;
- assert meaningful side effects, such as files written under the mount point,
  xattrs, REST calls, stream measurements, or status logs;
- keep SDK runtime behaviour out of handler tests unless it is part of the
  lambda logic.

Add `test_runtime.py` only when it checks something useful beyond the same
handler assertions:

- mounted file access through `ONECLIENT_MOUNT_POINT`;
- REST request flow using the runtime request envelope;
- xattr or filesystem side effects observable only end to end;
- stream flushing and final `result.streams`;
- batch behaviour, result ordering, or isolation of per-job exceptions;
- archive or multi-step workflows where `build_request()` plus `run_local()`
  exercises a realistic lambda invocation.

Do not add runtime tests for very small pure transformations when
`test_handler.py` already covers the behaviour. A runtime test that only asserts
`{"resultsBatch": [...]}` for a trivial handler is usually not worth keeping.

When adding tests, keep them deterministic. Avoid depending on external network
access, real providers, wall-clock time, or host-specific files. Mock REST
clients and xattr access locally, create mounted files under `tmp_path`, and
verify both the returned values and the important side effects.

## Contributing

To add or change a lambda or workflow schema:

1. Develop against the dev registry (`docker.onedata.org`) — point the workflow
   JSON's `dockerImage` at your dev image while testing.
2. **Bump the lambda's `version`** in its `pyproject.toml` whenever you change
   it; the image tag follows the version, so this avoids overwriting a published
   image.
3. Before merging, make sure every image referenced by a workflow schema is
   public (`onedata/*`) and pushed — `make publish-all REGISTRY=onedata`, and use
   the `utils/` scripts to sync `dockerImage` references and recompute workflow
   checksums.
