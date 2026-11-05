# Automation examples

Example automation lambdas and workflow schemas for [Onedata](https://onedata.org).

This repository serves two purposes:

1. **Examples** to get started writing your own lambdas and workflows.
2. **Ready-to-use workflow schema dumps** (`<name>.json`) you can load straight
   into an automation inventory — download the JSON and use the *Upload JSON*
   action in the workflows tab.

The lambdas are built on **lambda-base v3** and the
**[`onedata-lambda-sdk`](https://pypi.org/project/onedata-lambda-sdk/) SDK**.
Their full authoring documentation (the handler API, testing, streaming, file
access) lives with the SDK, under `docs/` in `onedata-lambda-sdk`.

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
in `onedata-lambda-sdk`.

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
   `docs/guides/writing-a-handler.md` in `onedata-lambda-sdk`.
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
make publish-all REGISTRY=onedata                 # push all public images (onedata/*)
make publish-all REGISTRY=onedata YES=1           # skip existing-tag confirmation
make check-lambda-image-matches-registry LAMBDA=calculate-checksum-mounted REGISTRY=onedata
make check-lambda-image-matches-registry LAMBDA=all REGISTRY=onedata
```

`make build LAMBDA=<name>` is a thin wrapper over
`docker build --build-arg LAMBDA_PACKAGE=<name> .`; the image gets only that
lambda's dependency subtree. The tag's version comes from the member's
`pyproject.toml`. Run `make help` (or `make image-name LAMBDA=<name>`) for
details. The image comparison target resolves the same name and version, then
checks that the locally built image has the same content digest as the image
published under that tag.

## Development

```bash
make sync     # uv sync --all-packages — one .venv with every member, editable
make check    # lint (ruff + mypy) + tests across the workspace
make test     # pytest only
```

## Testing guidelines

Each lambda should have focused tests under `lambdas/<name>/tests/`. There are
two complementary types of tests.

Tests of repository automation tools live under `utils/tests/`. They are
discovered by the same `make test` command and do not need a separate CI job.

`test_handler.py` contains unit tests of the lambda's own logic. These tests call
the handler directly, without starting the SDK runtime. `build_jobs()` and
`build_job_context()` prepare the required input and context. Calls that the
handler makes to loggers, streamers, and heartbeats are captured in memory so
that tests can inspect them.

Use `test_handler.py` to:

- test successful results as well as validation failures, error handling,
  per-job exceptions, and other error paths;
- test `handle()` or individual helper functions from the lambda;
- mock external services and other dependencies;
- check returned values and side effects such as created files, xattrs, REST
  calls, logs, and streamed items.

These tests confirm what the handler sends to loggers and streamers, but they do
not test how the SDK buffers, writes, or flushes that data.

`test_runtime.py` contains end-to-end tests of the lambda and SDK working
together. `build_request()` creates a request in the same format as the backend,
and `run_local()` passes it through the real SDK runtime. This covers request
parsing, `JobContext` creation, the decorated handler, stream output, and the
final response. It does not start Docker, but it is the closest local equivalent
of a real lambda invocation.

Use `test_runtime.py` to:

- invoke only the exported handler through `build_request()` and `run_local()`;
- leave the handler and SDK unchanged, while mocking systems outside the lambda
  such as REST services, filesystem integrations, xattr access, or optional
  native clients;
- check the final response, flushed streams, output files, and other externally
  visible effects.

A runtime test is most valuable when it checks SDK behaviour that a handler unit
test does not. Repeating the same assertion through `run_local()` may add little
value for a very small lambda that only performs a simple transformation.

This division is a guideline rather than a strict rule.

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
   public (`onedata/*`), pushed, and current — use `make publish-all
   REGISTRY=onedata` followed by the workflow image validation targets. Recompute
   the workflow checksum after editing a dump.
