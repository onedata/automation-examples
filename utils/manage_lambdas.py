#!/usr/bin/env python3

"""Validate and guard publication of lambda Docker images.

Versions are stored in `lambdas/<name>/pyproject.toml` without the `v`
prefix. The root Makefile adds that prefix when constructing Docker image tags.
"""

from __future__ import annotations


__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import argparse
import json
import sys
from collections.abc import Sequence

from utils.management_utils import (
    MISSING_MANIFEST_MARKERS,
    LambdaProject,
    LambdaProjectError,
    command_output,
    discover_lambda_projects,
    run_docker,
)


class LambdaManagementError(Exception):
    """Raised when a requested lambda management operation cannot be completed."""


class DockerImageNotFound(LambdaManagementError):
    """Raised when an inspected Docker image does not exist."""


class OperationCancelled(Exception):
    """Raised when the user declines a potentially destructive operation."""


def main() -> int:
    args = parse_args()
    try:
        run(args)
    except (LambdaManagementError, LambdaProjectError, OperationCancelled) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments used by image and release Make targets."""

    parser = argparse.ArgumentParser(
        description="Validate lambda images and guard their publication.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    confirm_parser = subparsers.add_parser(
        "confirm-publish",
        help="check whether publishing would overwrite existing Docker tags",
    )
    confirm_parser.add_argument(
        "lambda_selector",
        metavar="LAMBDA",
        help="lambda directory name or 'all'",
    )
    confirm_parser.add_argument(
        "--registry",
        required=True,
        help="target Docker registry or namespace",
    )
    confirm_parser.add_argument(
        "--yes",
        action="store_true",
        help="allow overwriting without an interactive prompt",
    )

    compare_parser = subparsers.add_parser(
        "check-image-matches-registry",
        help="compare one or all local lambda images with their published images",
    )
    compare_parser.add_argument(
        "lambda_selector",
        metavar="LAMBDA",
        help="lambda directory name or 'all'",
    )
    compare_parser.add_argument(
        "--registry",
        required=True,
        help="Docker registry or namespace containing the published image",
    )

    return parser.parse_args()


def run(args: argparse.Namespace) -> None:
    """Execute the requested publication guard or image comparison command."""

    projects = discover_lambda_projects()
    selected = select_projects(projects, args.lambda_selector)

    if args.command == "confirm-publish":
        confirm_publish(selected, args.registry, args.yes)
        return

    if args.command == "check-image-matches-registry":
        assert_images_match_registry(selected, args.registry)


def confirm_publish(
    selected_projects: Sequence[LambdaProject],
    registry: str,
    assume_yes: bool,
) -> None:
    """Require confirmation before a push can reassign any existing remote tags."""

    registry = registry.rstrip("/")
    if not registry:
        raise LambdaManagementError("registry must not be empty")

    existing_images = []
    for project in selected_projects:
        image = f"{registry}/{project.image}"
        if docker_image_exists(image):
            existing_images.append(image)

    if not existing_images:
        print("No existing Docker tags would be overwritten.")
        return

    print(
        "WARNING: pushing this release may overwrite or reassign existing Docker tags:",
        file=sys.stderr,
    )
    for image in existing_images:
        print(f"  - {image}", file=sys.stderr)

    if assume_yes:
        print("Overwrite confirmation supplied with --yes.", file=sys.stderr)
        return

    try:
        answer = input("Continue and allow these tags to be overwritten? [y/N] ")
    except EOFError as error:
        raise OperationCancelled(
            "publication cancelled: explicit confirmation is required"
        ) from error
    if answer.strip().lower() not in {"y", "yes"}:
        raise OperationCancelled("publication cancelled by user")


def assert_images_match_registry(selected_projects: Sequence[LambdaProject], registry: str) -> None:
    """Compare every selected image and report all mismatches and lookup failures."""

    errors = []
    for project in selected_projects:
        try:
            assert_image_matches_registry(project, registry)
        except LambdaManagementError as error:
            errors.append(str(error))

    if errors:
        details = "\n\n".join(errors)
        raise LambdaManagementError(f"{len(errors)} lambda image comparison(s) failed:\n{details}")


def assert_image_matches_registry(project: LambdaProject, registry: str) -> None:
    """Check that the current locally built lambda image matches its remote tag."""

    registry = registry.rstrip("/")
    if not registry:
        raise LambdaManagementError("registry must not be empty")

    image = f"{registry}/{project.image}"
    local_digest = inspect_local_docker_image(image)
    published_digest = inspect_published_docker_image(image)
    if local_digest != published_digest:
        raise LambdaManagementError(
            f"local and published Docker images differ for lambda {project.name!r}:\n"
            f"  image:            {image}\n"
            f"  version/tag:      {project.version}\n"
            f"  local ID:         {local_digest}\n"
            f"  published config: {published_digest}"
        )

    print(f"Local and published Docker images match: {image} ({local_digest})")


def select_projects(
    projects: dict[str, LambdaProject], lambda_selector: str
) -> list[LambdaProject]:
    """Return one validated lambda project or every project for `all`."""

    if lambda_selector == "all":
        return list(projects.values())

    try:
        return [projects[lambda_selector]]
    except KeyError as error:
        available = ", ".join(projects)
        raise LambdaManagementError(
            f"unknown lambda {lambda_selector!r}; available lambdas: {available}"
        ) from error


def docker_image_exists(image: str) -> bool:
    """Return whether a remote tag exists, raising on access or registry errors."""

    try:
        inspect_docker_json(
            ["manifest", "inspect"],
            image,
            image_description="published Docker image",
            missing_markers=MISSING_MANIFEST_MARKERS,
        )
    except DockerImageNotFound:
        return False
    return True


def inspect_local_docker_image(image: str) -> str:
    """Read the config digest of an image from the local Docker daemon."""

    try:
        metadata = inspect_docker_json(
            ["image", "inspect"],
            image,
            image_description="local Docker image",
            missing_markers=("no such image", "no such object"),
        )
        digest = metadata[0]["Id"]  # type: ignore[index]
    except (IndexError, KeyError, TypeError) as error:
        raise LambdaManagementError(
            f"docker returned invalid local Docker image metadata for {image}: {error}"
        ) from error

    return validate_docker_digest(digest, image, "local Docker image")


def inspect_published_docker_image(image: str) -> str:
    """Read the config digest of a published single-platform image."""

    try:
        manifest = inspect_docker_json(
            ["manifest", "inspect"],
            image,
            image_description="published Docker image",
            missing_markers=MISSING_MANIFEST_MARKERS,
        )
        digest = manifest["config"]["digest"]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise LambdaManagementError(
            f"docker returned invalid published Docker image metadata for {image}: {error}"
        ) from error

    return validate_docker_digest(digest, image, "published Docker image")


def inspect_docker_json(
    command: Sequence[str],
    image: str,
    *,
    image_description: str,
    missing_markers: Sequence[str],
) -> object:
    """Run a Docker inspection command and return its decoded JSON output."""

    docker_arguments = [*command, image]
    try:
        result = run_docker(*docker_arguments)
    except OSError as error:
        command_name = " ".join(["docker", *docker_arguments])
        raise LambdaManagementError(f"cannot run {command_name}: {error}") from error

    if result.returncode != 0:
        error_output = command_output(result)
        normalized_error = error_output.lower()
        if any(marker in normalized_error for marker in missing_markers):
            raise DockerImageNotFound(f"{image_description} does not exist: {image}")
        detail = f": {error_output}" if error_output else ""
        raise LambdaManagementError(f"cannot inspect {image_description} {image}{detail}")

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise LambdaManagementError(
            f"docker returned invalid {image_description} metadata for {image}: {error}"
        ) from error


def validate_docker_digest(digest: object, image: str, image_description: str) -> str:
    """Validate a config digest extracted from Docker inspection metadata."""

    if not isinstance(digest, str) or not digest:
        raise LambdaManagementError(f"{image_description} has no config digest: {image}")
    return digest


if __name__ == "__main__":
    raise SystemExit(main())
