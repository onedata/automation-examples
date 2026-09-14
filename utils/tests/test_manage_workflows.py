"""Unit tests for the workflow image management utility."""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
from utils import management_utils


SCRIPT_PATH = Path(__file__).parents[1] / "manage_workflows.py"
SPEC = importlib.util.spec_from_file_location("manage_workflows", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
manage_workflows = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = manage_workflows
SPEC.loader.exec_module(manage_workflows)


@pytest.mark.parametrize("selector", [None, "", "all"])
def test_all_lambda_selectors_are_normalized(selector: str | None) -> None:
    assert manage_workflows.normalize_lambda_selector(selector) is None


@pytest.mark.parametrize(
    ("arguments", "expected_selector"),
    [
        (["check-public"], "all"),
        (["check-public", "example"], "example"),
        (["check-published", "all"], "all"),
        (["check-lambda-images-used", "example"], "example"),
    ],
)
def test_commands_accept_positional_lambda_selector(
    arguments: list[str], expected_selector: str, monkeypatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["manage_workflows.py", *arguments])

    args = manage_workflows.parse_args()

    assert args.lambda_selector == expected_selector


def test_discovers_nested_workflows_and_collects_unique_images(tmp_path: Path, monkeypatch) -> None:
    create_workflow(tmp_path, "first.json", ["onedata/lambda-first:v1"])
    create_workflow(
        tmp_path,
        "nested/second.json",
        ["onedata/lambda-first:v1", "onedata/lambda-second:v2"],
    )

    monkeypatch.setattr(management_utils, "REPO_ROOT", tmp_path)
    dumps = manage_workflows.discover_workflow_dumps()

    assert manage_workflows.collect_docker_images(dumps) == {
        "onedata/lambda-first:v1",
        "onedata/lambda-second:v2",
    }


def test_rejects_invalid_docker_image_value(tmp_path: Path, monkeypatch) -> None:
    workflow_path = tmp_path / "workflows" / "invalid.json"
    workflow_path.parent.mkdir()
    workflow_path.write_text('{"dockerImage": null}', encoding="utf-8")

    monkeypatch.setattr(management_utils, "REPO_ROOT", tmp_path)
    with pytest.raises(manage_workflows.WorkflowManagementError, match="non-empty string"):
        manage_workflows.discover_workflow_dumps()


def test_load_current_lambda_images_reads_project_name_from_pyproject(
    tmp_path: Path, monkeypatch
) -> None:
    pyproject_path = tmp_path / "lambdas" / "directory-name" / "pyproject.toml"
    pyproject_path.parent.mkdir(parents=True)
    pyproject_path.write_text(
        '[project]\nname = "project-name"\nversion = "4-dev1"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(management_utils, "REPO_ROOT", tmp_path)

    images = manage_workflows.load_current_lambda_images()

    assert images == {"lambda-project-name": "lambda-project-name:v4-dev1"}


def test_lambda_usage_ignores_external_and_obsolete_images() -> None:
    with pytest.raises(manage_workflows.WorkflowImageValidationError) as error:
        manage_workflows.assert_all_lambda_images_used(
            {
                "onedata/lambda-first:v1",
                "onedata/lambda-obsolete:v1",
                "docker.io/library/busybox:latest",
            },
            {
                "lambda-first": "lambda-first:v1",
                "lambda-missing": "lambda-missing:v2",
            },
        )

    assert error.value.errors == ["lambda image is not used in any workflow: lambda-missing:v2"]


def test_single_lambda_usage_does_not_report_other_lambda_images() -> None:
    manage_workflows.assert_all_lambda_images_used(
        {
            "onedata/lambda-selected:v2",
            "onedata/lambda-other:v1",
        },
        {"lambda-selected": "lambda-selected:v2"},
    )


@pytest.mark.parametrize(
    ("returncode", "stderr", "expected_status"),
    [
        (0, "", "published"),
        (1, "manifest unknown", "missing"),
        (1, "unauthorized", "unauthorized"),
        (1, "connection refused", "error"),
    ],
)
def test_manifest_status(monkeypatch, returncode: int, stderr: str, expected_status: str) -> None:
    result = subprocess.CompletedProcess([], returncode, stdout="", stderr=stderr)
    monkeypatch.setattr(manage_workflows, "run_docker", lambda *args: result)

    status, _ = manage_workflows.inspect_manifest("onedata/lambda-example:v1")

    assert status == expected_status


def create_workflow(
    repo_root: Path,
    relative_path: str,
    images: list[str],
    **extra_content,
) -> Path:
    workflow_path = repo_root / "workflows" / relative_path
    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    workflow_path.write_text(
        json.dumps(
            {
                **extra_content,
                "tasks": [{"dockerImage": image} for image in images],
            }
        ),
        encoding="utf-8",
    )
    return workflow_path
