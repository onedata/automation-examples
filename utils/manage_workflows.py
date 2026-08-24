#!/usr/bin/env python3

"""Validate lambda image references in workflow JSON dumps."""

from __future__ import annotations


__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import argparse
import json
import subprocess
import sys
import tomllib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PUBLIC_REGISTRY = "onedata"


class WorkflowManagementError(Exception):
    """Raised when a workflow management operation cannot be completed."""


class WorkflowImageValidationError(WorkflowManagementError):
    """Raised for one or more failed workflow image assertions."""

    def __init__(self, errors: Sequence[str]) -> None:
        """Initialize the exception with individual validation errors."""

        super().__init__("workflow image validation failed")
        self.errors = list(errors)


@dataclass
class WorkflowDump:
    """Parsed workflow JSON together with its source path."""

    path: Path
    content: Any

    def docker_images(self) -> Iterator[str]:
        """Iterate over Docker image references in this workflow."""

        yield from iter_docker_images(self.content, self.path)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface and return its exit code."""

    args = create_parser().parse_args(argv)
    try:
        run(args)
    except WorkflowImageValidationError as error:
        for message in error.errors:
            print(f"Error: {message}", file=sys.stderr)
        return 1
    except WorkflowManagementError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


def run(args: argparse.Namespace) -> None:
    """Execute the selected workflow image update or validation command."""

    workflow_dumps = discover_workflow_dumps()
    workflow_images = collect_docker_images(workflow_dumps)
    lambda_name = normalize_lambda_selector(args.lambda_selector)

    if args.command == "check-public":
        images_to_check = workflow_images
        if lambda_name:
            selected_images = select_current_lambda_images(
                load_current_lambda_images(), lambda_name
            )
            images_to_check = workflow_images_for_repositories(
                workflow_images, set(selected_images)
            )
        assert_public_registry(images_to_check, args.registry)
    elif args.command == "check-published":
        images_to_check = workflow_images
        if lambda_name:
            selected_images = select_current_lambda_images(
                load_current_lambda_images(), lambda_name
            )
            images_to_check = workflow_images_for_repositories(
                workflow_images, set(selected_images)
            )
        assert_published(images_to_check)
    elif args.command == "check-lambda-images-used":
        selected_images = select_current_lambda_images(load_current_lambda_images(), lambda_name)
        assert_all_lambda_images_used(
            workflow_images,
            selected_images,
        )


def create_parser() -> argparse.ArgumentParser:
    """Build the command-line interface used by workflow-related Make targets."""

    parser = argparse.ArgumentParser(
        description="Validate lambda image references in workflow JSON dumps."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    public_parser = subparsers.add_parser(
        "check-public",
        help="check that lambda images in workflows use the public registry",
    )
    public_parser.add_argument("--registry", default=DEFAULT_PUBLIC_REGISTRY)
    add_lambda_selector(public_parser, "check")

    published_parser = subparsers.add_parser(
        "check-published",
        help="check that lambda images in workflows are published",
    )
    add_lambda_selector(published_parser, "check")
    used_parser = subparsers.add_parser(
        "check-lambda-images-used",
        help="check that every current lambda image is used in a workflow",
    )
    add_lambda_selector(used_parser, "check")
    return parser


def add_lambda_selector(parser: argparse.ArgumentParser, action: str) -> None:
    """Add the shared optional lambda selector to a subcommand parser."""

    parser.add_argument(
        "lambda_selector",
        nargs="?",
        default="all",
        metavar="LAMBDA",
        help=f"{action} one lambda by name, or use 'all' (default: all)",
    )


def assert_public_registry(images: set[str], public_registry: str) -> None:
    """Require every selected workflow image to use the public registry prefix."""

    public_prefix = f"{public_registry.rstrip('/')}/"
    errors = [
        f"lambda image used in a workflow does not use {public_prefix}: {image}"
        for image in sorted(images)
        if not image.startswith(public_prefix)
    ]
    if errors:
        raise WorkflowImageValidationError(errors)
    print(f"All lambda images used in workflows use {public_prefix}.")


def assert_published(images: set[str]) -> None:
    """Require every selected workflow image manifest to be remotely accessible."""

    errors = []
    for image in sorted(images):
        status, detail = inspect_manifest(image)
        if status == "published":
            continue
        if status == "missing":
            message = f"Docker image is not published: {image}"
        elif status == "unauthorized":
            message = f"cannot access Docker image (authentication or permissions): {image}"
        else:
            message = f"could not verify Docker image (registry, network, or Docker error): {image}"
        errors.append(f"{message}\n  docker: {detail}" if detail else message)

    if errors:
        raise WorkflowImageValidationError(errors)
    print("All lambda images used in workflows are published and accessible.")


def assert_all_lambda_images_used(
    workflow_images: set[str],
    current_lambda_images: dict[str, str],
) -> None:
    """Require every selected current lambda image to occur in a workflow."""

    expected_images = set(current_lambda_images.values())
    used_lambda_images = {
        image.rsplit("/", maxsplit=1)[-1]
        for image in workflow_images
        if image_repository(image).startswith("lambda-")
    }

    errors = [
        f"lambda image is not used in any workflow: {image}"
        for image in sorted(expected_images - used_lambda_images)
    ]
    if errors:
        raise WorkflowImageValidationError(errors)
    print("Every selected lambda image is used in at least one workflow.")


def discover_workflow_dumps() -> list[WorkflowDump]:
    """Load every workflow JSON recursively and require at least one image reference."""

    dumps_dir = workflows_dir()
    if not dumps_dir.is_dir():
        raise WorkflowManagementError(f"workflow directory does not exist: {dumps_dir}")

    paths = sorted(dumps_dir.rglob("*.json"))
    if not paths:
        raise WorkflowManagementError(f"no workflow JSON files found under {dumps_dir}")

    dumps = [load_workflow_dump(path) for path in paths]
    found_docker_image = False
    for workflow_dump in dumps:
        if list(workflow_dump.docker_images()):
            found_docker_image = True
    if not found_docker_image:
        raise WorkflowManagementError("no dockerImage fields found in workflow JSON files")
    return dumps


def collect_docker_images(workflow_dumps: Sequence[WorkflowDump]) -> set[str]:
    """Collect unique Docker image references from workflow dumps."""

    return {image for workflow_dump in workflow_dumps for image in workflow_dump.docker_images()}


def load_current_lambda_images() -> dict[str, str]:
    """Resolve current lambda image names and tags from their project versions."""

    projects_dir = lambdas_dir()
    if not projects_dir.is_dir():
        raise WorkflowManagementError(f"lambda directory does not exist: {projects_dir}")

    images = {}
    for pyproject_path in sorted(projects_dir.glob("*/pyproject.toml")):
        try:
            with pyproject_path.open("rb") as pyproject_file:
                project = tomllib.load(pyproject_file)["project"]
            name = project["name"]
            version = project["version"]
        except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
            raise WorkflowManagementError(
                f"cannot read project metadata from {pyproject_path}: {error}"
            ) from error
        if not isinstance(name, str) or not name:
            raise WorkflowManagementError(
                f"project.name in {pyproject_path} must be a non-empty string"
            )
        if not isinstance(version, str) or not version:
            raise WorkflowManagementError(
                f"project.version in {pyproject_path} must be a non-empty string"
            )

        repository = f"lambda-{name}"
        images[repository] = f"{repository}:v{version}"

    if not images:
        raise WorkflowManagementError(f"no lambda pyproject.toml files found under {projects_dir}")
    return images


def select_current_lambda_images(
    current_lambda_images: dict[str, str],
    lambda_name: str | None,
) -> dict[str, str]:
    """Return all current images or validate and return one selected lambda image."""

    if lambda_name is None:
        return current_lambda_images

    repository = f"lambda-{lambda_name}"
    try:
        return {repository: current_lambda_images[repository]}
    except KeyError as error:
        available = ", ".join(
            repository.removeprefix("lambda-") for repository in current_lambda_images
        )
        raise WorkflowManagementError(
            f"unknown lambda {lambda_name!r}; available lambdas: {available}"
        ) from error


def workflow_images_for_repositories(
    workflow_images: set[str],
    repositories: set[str],
) -> set[str]:
    """Filter workflow images by repository name."""

    return {image for image in workflow_images if image_repository(image) in repositories}


def load_workflow_dump(path: Path) -> WorkflowDump:
    """Load and parse one workflow JSON file."""

    try:
        with path.open(encoding="utf-8") as workflow_file:
            content = json.load(workflow_file)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise WorkflowManagementError(f"cannot read workflow JSON {path}: {error}") from error
    return WorkflowDump(path=path, content=content)


def iter_docker_images(value: Any, path: Path) -> Iterator[str]:
    """Recursively yield valid ``dockerImage`` values from workflow data."""

    if isinstance(value, dict):
        for key, child in value.items():
            if key == "dockerImage":
                if not isinstance(child, str) or not child:
                    raise WorkflowManagementError(
                        f"dockerImage in {path} must be a non-empty string"
                    )
                yield child
            else:
                yield from iter_docker_images(child, path)
    elif isinstance(value, list):
        for child in value:
            yield from iter_docker_images(child, path)


def image_repository(image: str) -> str:
    """Return an image repository name without registry path or tag."""

    image_name = image.rsplit("/", maxsplit=1)[-1]
    repository, separator, _ = image_name.rpartition(":")
    return repository if separator else image_name


def inspect_manifest(image: str) -> tuple[str, str]:
    """Classify a remote manifest as published, missing, unauthorized or erroneous."""

    try:
        result = subprocess.run(
            ["docker", "manifest", "inspect", image],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        return "error", str(error)

    if result.returncode == 0:
        return "published", ""

    output = "\n".join(part for part in (result.stderr, result.stdout) if part).strip()
    normalized_output = output.lower()
    if "manifest unknown" in normalized_output or "no such manifest" in normalized_output:
        return "missing", output
    if any(
        marker in normalized_output
        for marker in ("unauthorized", "authentication required", "denied")
    ):
        return "unauthorized", output
    return "error", output


def normalize_lambda_selector(lambda_name: str | None) -> str | None:
    """Represent an empty or ``all`` selector as no lambda filter."""

    return None if not lambda_name or lambda_name == "all" else lambda_name


def workflows_dir() -> Path:
    """Return the repository directory containing workflow dumps."""

    return REPO_ROOT / "workflows"


def lambdas_dir() -> Path:
    """Return the repository directory containing lambda projects."""

    return REPO_ROOT / "lambdas"


if __name__ == "__main__":
    raise SystemExit(main())
