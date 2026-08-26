"""Shared helpers for lambda and workflow management commands."""

from __future__ import annotations


__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
VERSION_PATTERN = re.compile(r"^(?:0|[1-9][0-9]*)(?:-dev[1-9][0-9]*)?$")
MISSING_MANIFEST_MARKERS = ("manifest unknown", "no such manifest")


class LambdaProjectError(Exception):
    """Raised when lambda project metadata cannot be loaded or validated."""


@dataclass(frozen=True)
class LambdaProject:
    """Lambda name and validated image version."""

    name: str
    version: str

    @property
    def repository(self) -> str:
        """Return the Docker repository name for this lambda."""

        return f"lambda-{self.name}"

    @property
    def image(self) -> str:
        """Return the versioned Docker image name for this lambda."""

        return f"{self.repository}:v{self.version}"


def discover_lambda_projects() -> dict[str, LambdaProject]:
    """Load lambda projects keyed by their directory names."""

    projects_dir = lambdas_dir()
    if not projects_dir.is_dir():
        raise LambdaProjectError(f"lambda directory does not exist: {projects_dir}")

    projects = {
        path.parent.name: load_lambda_project(path)
        for path in sorted(projects_dir.glob("*/pyproject.toml"))
    }
    if not projects:
        raise LambdaProjectError(f"no lambda pyproject.toml files found under {projects_dir}")
    return projects


def load_lambda_project(pyproject_path: Path) -> LambdaProject:
    """Read and validate one lambda's project name and version."""

    try:
        with pyproject_path.open("rb") as pyproject_file:
            project = tomllib.load(pyproject_file)["project"]
        name = project["name"]
        version = project["version"]
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        raise LambdaProjectError(
            f"cannot read project metadata from {pyproject_path}: {error}"
        ) from error

    if not isinstance(name, str) or not name:
        raise LambdaProjectError(f"project.name in {pyproject_path} must be a non-empty string")
    if not isinstance(version, str):
        raise LambdaProjectError(f"project.version in {pyproject_path} must be a string")

    return LambdaProject(name=name, version=validate_version(version))


def validate_version(value: str) -> str:
    """Validate and return a version used to construct a lambda image tag."""

    if VERSION_PATTERN.fullmatch(value) is None:
        raise LambdaProjectError(
            f"unsupported version {value!r}; expected <release> or <release>-dev<number>"
        )
    return value


def run_docker(*arguments: str) -> subprocess.CompletedProcess[str]:
    """Run a Docker command and capture its text output."""

    return subprocess.run(
        ["docker", *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def command_output(result: subprocess.CompletedProcess[str]) -> str:
    """Combine captured stderr and stdout into one error description."""

    return "\n".join(part for part in (result.stderr, result.stdout) if part).strip()


def manifest_is_missing(output: str) -> bool:
    """Return whether Docker reports that a manifest does not exist."""

    normalized_output = output.lower()
    return any(marker in normalized_output for marker in MISSING_MANIFEST_MARKERS)


def lambdas_dir() -> Path:
    """Return the repository directory containing lambda projects."""

    return REPO_ROOT / "lambdas"


def workflows_dir() -> Path:
    """Return the repository directory containing workflow dumps."""

    return REPO_ROOT / "workflows"
