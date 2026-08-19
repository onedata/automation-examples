"""Unit tests for the lambda image management utility."""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).parents[1] / "manage_lambdas.py"
SPEC = importlib.util.spec_from_file_location("manage_lambdas", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
manage_lambdas = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = manage_lambdas
SPEC.loader.exec_module(manage_lambdas)


@pytest.mark.parametrize("version", ["4", "4-dev2"])
def test_accepts_supported_version(version: str) -> None:
    assert manage_lambdas.validate_version(version) == version


@pytest.mark.parametrize("version", ["v4-dev2", "4-dev", "4-dev0", "4.1", "04-dev2"])
def test_rejects_unsupported_version(version: str) -> None:
    with pytest.raises(manage_lambdas.VersionManagementError):
        manage_lambdas.validate_version(version)


def test_select_projects_accepts_all(tmp_path: Path) -> None:
    create_lambda(tmp_path, "first", "4-dev1")
    create_lambda(tmp_path, "second", "5-dev1")
    projects = manage_lambdas.discover_lambda_projects(tmp_path)

    selected = manage_lambdas.select_projects(projects, "all")

    assert [project.name for project in selected] == ["first", "second"]


def test_docker_image_exists_distinguishes_missing_manifest(monkeypatch) -> None:
    result = subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout="",
        stderr="manifest unknown",
    )
    monkeypatch.setattr(manage_lambdas.subprocess, "run", lambda *args, **kwargs: result)

    assert not manage_lambdas.docker_image_exists("onedata/lambda-example:v5")


def test_docker_image_exists_rejects_registry_errors(monkeypatch) -> None:
    result = subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout="",
        stderr="unauthorized: authentication required",
    )
    monkeypatch.setattr(manage_lambdas.subprocess, "run", lambda *args, **kwargs: result)

    with pytest.raises(manage_lambdas.VersionManagementError, match="cannot verify"):
        manage_lambdas.docker_image_exists("onedata/lambda-example:v5")


def test_confirm_publish_requires_confirmation_for_existing_tags(monkeypatch, capsys) -> None:
    project = manage_lambdas.LambdaProject(
        name="example",
        version="4-dev2",
    )
    inspected_images = []

    def image_exists(image: str) -> bool:
        inspected_images.append(image)
        return True

    monkeypatch.setattr(manage_lambdas, "docker_image_exists", image_exists)
    monkeypatch.setattr("builtins.input", lambda _: "no")

    with pytest.raises(manage_lambdas.OperationCancelled):
        manage_lambdas.confirm_publish([project], "onedata", False)

    assert inspected_images == ["onedata/lambda-example:v4-dev2"]
    assert "WARNING" in capsys.readouterr().err


def test_confirm_publish_does_not_prompt_for_new_tags(monkeypatch, capsys) -> None:
    project = manage_lambdas.LambdaProject(
        name="example",
        version="4-dev2",
    )
    monkeypatch.setattr(manage_lambdas, "docker_image_exists", lambda _: False)
    monkeypatch.setattr(
        "builtins.input",
        lambda _: pytest.fail("confirmation should not be requested"),
    )

    manage_lambdas.confirm_publish([project], "onedata", False)

    assert "No existing Docker tags" in capsys.readouterr().out


def test_confirm_publish_can_skip_prompt_for_existing_tags(monkeypatch, capsys) -> None:
    project = lambda_project("example", "4-dev2")
    monkeypatch.setattr(manage_lambdas, "docker_image_exists", lambda _: True)
    monkeypatch.setattr(
        "builtins.input",
        lambda _: pytest.fail("confirmation should be skipped"),
    )

    manage_lambdas.confirm_publish([project], "onedata", True)

    error_output = capsys.readouterr().err
    assert "WARNING" in error_output
    assert "confirmation supplied with --yes" in error_output


def test_assert_image_matches_registry_for_single_platform(monkeypatch, capsys) -> None:
    project = lambda_project("example", "4-dev2")
    local_image = manage_lambdas.LocalDockerImage(
        image_id="sha256:config",
        os="linux",
        architecture="amd64",
    )
    monkeypatch.setattr(manage_lambdas, "inspect_local_docker_image", lambda _: local_image)
    monkeypatch.setattr(
        manage_lambdas,
        "inspect_remote_manifest",
        lambda _: {"config": {"digest": "sha256:config"}},
    )

    manage_lambdas.assert_image_matches_registry(project, "onedata/")

    output = capsys.readouterr().out
    assert "Local and published Docker images match" in output
    assert "onedata/lambda-example:v4-dev2" in output


def test_assert_image_matches_registry_rejects_different_images(monkeypatch) -> None:
    project = lambda_project("example", "4-dev2")
    local_image = manage_lambdas.LocalDockerImage(
        image_id="sha256:local",
        os="linux",
        architecture="amd64",
    )
    monkeypatch.setattr(manage_lambdas, "inspect_local_docker_image", lambda _: local_image)
    monkeypatch.setattr(
        manage_lambdas,
        "inspect_remote_manifest",
        lambda _: {"config": {"digest": "sha256:published"}},
    )

    with pytest.raises(manage_lambdas.VersionManagementError, match="images differ") as error:
        manage_lambdas.assert_image_matches_registry(project, "onedata")

    assert "sha256:local" in str(error.value)
    assert "sha256:published" in str(error.value)


def test_assert_images_match_registry_reports_all_failures(monkeypatch) -> None:
    projects = [lambda_project("first", "1"), lambda_project("second", "2")]
    checked_projects = []

    def fail_comparison(project, registry: str) -> None:
        checked_projects.append((project.name, registry))
        raise manage_lambdas.VersionManagementError(f"{project.name} differs")

    monkeypatch.setattr(manage_lambdas, "assert_image_matches_registry", fail_comparison)

    with pytest.raises(
        manage_lambdas.VersionManagementError, match=r"2 lambda image comparison\(s\) failed"
    ) as error:
        manage_lambdas.assert_images_match_registry(projects, "onedata")

    assert checked_projects == [("first", "onedata"), ("second", "onedata")]
    assert "first differs" in str(error.value)
    assert "second differs" in str(error.value)


def test_remote_config_digest_selects_local_platform_from_image_index(monkeypatch) -> None:
    image = "registry.example:5000/lambda-example:v4"
    local_image = manage_lambdas.LocalDockerImage(
        image_id="sha256:amd64-config",
        os="linux",
        architecture="amd64",
    )
    inspected_images = []

    def inspect_manifest(reference: str):
        inspected_images.append(reference)
        if reference == image:
            return {
                "manifests": [
                    {
                        "digest": "sha256:arm64-manifest",
                        "platform": {"os": "linux", "architecture": "arm64"},
                    },
                    {
                        "digest": "sha256:amd64-manifest",
                        "platform": {"os": "linux", "architecture": "amd64"},
                    },
                ]
            }
        return {"config": {"digest": "sha256:amd64-config"}}

    monkeypatch.setattr(manage_lambdas, "inspect_remote_manifest", inspect_manifest)

    digest = manage_lambdas.remote_config_digest(image, local_image)

    assert digest == "sha256:amd64-config"
    assert inspected_images == [
        image,
        "registry.example:5000/lambda-example@sha256:amd64-manifest",
    ]


def test_remote_config_digest_rejects_missing_published_image(monkeypatch) -> None:
    local_image = manage_lambdas.LocalDockerImage(
        image_id="sha256:local",
        os="linux",
        architecture="amd64",
    )
    monkeypatch.setattr(manage_lambdas, "inspect_remote_manifest", lambda _: None)

    with pytest.raises(manage_lambdas.VersionManagementError, match="does not exist"):
        manage_lambdas.remote_config_digest("onedata/lambda-example:v4", local_image)


def test_inspect_local_docker_image_rejects_missing_image(monkeypatch) -> None:
    result = subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout="",
        stderr="Error: No such image: onedata/lambda-example:v4",
    )
    monkeypatch.setattr(manage_lambdas.subprocess, "run", lambda *args, **kwargs: result)

    with pytest.raises(manage_lambdas.VersionManagementError, match="local Docker image"):
        manage_lambdas.inspect_local_docker_image("onedata/lambda-example:v4")


def test_inspect_local_docker_image_reads_platform_and_id(monkeypatch) -> None:
    result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(
            [
                {
                    "Id": "sha256:config",
                    "Os": "linux",
                    "Architecture": "arm64",
                    "Variant": "v8",
                }
            ]
        ),
        stderr="",
    )
    monkeypatch.setattr(manage_lambdas.subprocess, "run", lambda *args, **kwargs: result)

    image = manage_lambdas.inspect_local_docker_image("onedata/lambda-example:v4")

    assert image.image_id == "sha256:config"
    assert image.platform == "linux/arm64/v8"


def lambda_project(name: str, version: str):
    return manage_lambdas.LambdaProject(
        name=name,
        version=manage_lambdas.validate_version(version),
    )


def create_lambda(repo_root: Path, name: str, version: str) -> Path:
    lambda_dir = repo_root / "lambdas" / name
    lambda_dir.mkdir(parents=True)
    pyproject_path = lambda_dir / "pyproject.toml"
    pyproject_path.write_text(
        f'[project]\nname = "{name}"\nversion = "{version}"\n',
        encoding="utf-8",
    )
    return pyproject_path
