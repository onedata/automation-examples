#!/usr/bin/env python3

"""Validate and guard publication of lambda Docker images.

Versions are stored in ``lambdas/<name>/pyproject.toml`` without the ``v``
prefix. The root Makefile adds that prefix when constructing Docker image tags.
"""

from __future__ import annotations


__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import argparse
import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
VERSION_PATTERN = re.compile(r"^(?:0|[1-9][0-9]*)(?:-dev[1-9][0-9]*)?$")


class VersionManagementError(Exception):
    """Raised when a requested version operation cannot be completed."""


class OperationCancelled(Exception):
    """Raised when the user declines a potentially destructive operation."""


def validate_version(value: str) -> str:
    """Validate and return a version used to construct a lambda image tag."""

    if VERSION_PATTERN.fullmatch(value) is None:
        raise VersionManagementError(
            f"unsupported version {value!r}; expected <release> or <release>-dev<number>"
        )
    return value


@dataclass(frozen=True)
class LambdaProject:
    """Lambda name and validated image version."""

    name: str
    version: str

    @property
    def image(self) -> str:
        return f"lambda-{self.name}:v{self.version}"


@dataclass(frozen=True)
class LocalDockerImage:
    """Local image identity and platform used for registry comparison."""

    image_id: str
    os: str
    architecture: str
    variant: str | None = None

    @property
    def platform(self) -> str:
        suffix = f"/{self.variant}" if self.variant else ""
        return f"{self.os}/{self.architecture}{suffix}"


def discover_lambda_projects(repo_root: Path) -> dict[str, LambdaProject]:
    """Load every lambda and its version from ``lambdas/*/pyproject.toml``."""

    lambdas_dir = repo_root / "lambdas"
    if not lambdas_dir.is_dir():
        raise VersionManagementError(f"lambda directory does not exist: {lambdas_dir}")

    projects = {
        path.parent.name: load_lambda_project(path)
        for path in sorted(lambdas_dir.glob("*/pyproject.toml"))
    }
    if not projects:
        raise VersionManagementError(f"no lambda pyproject.toml files found under {lambdas_dir}")
    return projects


def load_lambda_project(pyproject_path: Path) -> LambdaProject:
    """Read and validate one lambda's ``project.version``."""

    try:
        with pyproject_path.open("rb") as pyproject_file:
            pyproject = tomllib.load(pyproject_file)
        version_value = pyproject["project"]["version"]
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        raise VersionManagementError(
            f"cannot read project version from {pyproject_path}: {error}"
        ) from error

    if not isinstance(version_value, str):
        raise VersionManagementError(f"project.version in {pyproject_path} must be a string")

    return LambdaProject(
        name=pyproject_path.parent.name,
        version=validate_version(version_value),
    )


def select_projects(
    projects: dict[str, LambdaProject], lambda_selector: str
) -> list[LambdaProject]:
    """Return one validated lambda project or every project for ``all``."""

    if lambda_selector == "all":
        return list(projects.values())

    try:
        return [projects[lambda_selector]]
    except KeyError as error:
        available = ", ".join(projects)
        raise VersionManagementError(
            f"unknown lambda {lambda_selector!r}; available lambdas: {available}"
        ) from error


def docker_image_exists(image: str) -> bool:
    """Return whether a remote tag exists, raising on access or registry errors."""

    try:
        result = subprocess.run(
            ["docker", "manifest", "inspect", image],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise VersionManagementError(f"cannot run docker manifest inspect: {error}") from error

    if result.returncode == 0:
        return True

    error_output = "\n".join(part for part in (result.stderr, result.stdout) if part).strip()
    normalized_error = error_output.lower()
    if "manifest unknown" in normalized_error or "no such manifest" in normalized_error:
        return False

    detail = f": {error_output}" if error_output else ""
    raise VersionManagementError(f"cannot verify whether {image} exists{detail}")


def inspect_local_docker_image(image: str) -> LocalDockerImage:
    """Read the content ID and platform of an image from the local Docker daemon."""

    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise VersionManagementError(f"cannot run docker image inspect: {error}") from error

    if result.returncode != 0:
        error_output = "\n".join(part for part in (result.stderr, result.stdout) if part).strip()
        normalized_error = error_output.lower()
        if "no such image" in normalized_error or "no such object" in normalized_error:
            raise VersionManagementError(f"local Docker image does not exist: {image}")
        detail = f": {error_output}" if error_output else ""
        raise VersionManagementError(f"cannot inspect local Docker image {image}{detail}")

    try:
        payload = json.loads(result.stdout)
        details = payload[0]
        image_id = details["Id"]
        image_os = details["Os"]
        architecture = details["Architecture"]
        variant = details.get("Variant") or None
    except (json.JSONDecodeError, IndexError, KeyError, TypeError) as error:
        raise VersionManagementError(
            f"docker returned invalid local image metadata for {image}: {error}"
        ) from error

    if not all(isinstance(value, str) and value for value in (image_id, image_os, architecture)):
        raise VersionManagementError(f"docker returned incomplete local image metadata for {image}")
    if variant is not None and not isinstance(variant, str):
        raise VersionManagementError(f"docker returned an invalid platform variant for {image}")

    return LocalDockerImage(
        image_id=image_id,
        os=image_os,
        architecture=architecture,
        variant=variant,
    )


def inspect_remote_manifest(image: str) -> dict[str, object] | None:
    """Read a registry manifest, returning ``None`` only when it does not exist."""

    try:
        result = subprocess.run(
            ["docker", "manifest", "inspect", image],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise VersionManagementError(f"cannot run docker manifest inspect: {error}") from error

    if result.returncode != 0:
        error_output = "\n".join(part for part in (result.stderr, result.stdout) if part).strip()
        normalized_error = error_output.lower()
        if "manifest unknown" in normalized_error or "no such manifest" in normalized_error:
            return None
        detail = f": {error_output}" if error_output else ""
        raise VersionManagementError(f"cannot inspect published Docker image {image}{detail}")

    try:
        manifest = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise VersionManagementError(
            f"docker returned an invalid manifest for {image}: {error}"
        ) from error
    if not isinstance(manifest, dict):
        raise VersionManagementError(f"docker returned an invalid manifest for {image}")
    return manifest


def manifest_config_digest(manifest: dict[str, object], image: str) -> str:
    config = manifest.get("config")
    if not isinstance(config, dict):
        raise VersionManagementError(f"published manifest has no image config: {image}")
    digest = config.get("digest")
    if not isinstance(digest, str) or not digest:
        raise VersionManagementError(f"published manifest has no config digest: {image}")
    return digest


def repository_from_image_reference(image: str) -> str:
    without_digest = image.split("@", maxsplit=1)[0]
    last_slash = without_digest.rfind("/")
    last_colon = without_digest.rfind(":")
    if last_colon > last_slash:
        return without_digest[:last_colon]
    return without_digest


def select_platform_manifest(
    descriptors: object,
    local_image: LocalDockerImage,
    image: str,
) -> dict[str, object]:
    if not isinstance(descriptors, list):
        raise VersionManagementError(f"published manifest has an unsupported format: {image}")

    platform_matches: list[dict[str, object]] = []
    matches_without_variant: list[dict[str, object]] = []
    for descriptor in descriptors:
        if not isinstance(descriptor, dict):
            continue
        platform = descriptor.get("platform")
        if not isinstance(platform, dict):
            continue
        if (
            platform.get("os") != local_image.os
            or platform.get("architecture") != local_image.architecture
        ):
            continue
        if local_image.variant and platform.get("variant") != local_image.variant:
            continue
        platform_matches.append(descriptor)
        if not platform.get("variant"):
            matches_without_variant.append(descriptor)

    if not local_image.variant and matches_without_variant:
        platform_matches = matches_without_variant
    if not platform_matches:
        raise VersionManagementError(
            f"published image {image} has no manifest for local platform {local_image.platform}"
        )
    if len(platform_matches) > 1:
        raise VersionManagementError(
            f"published image {image} has multiple manifests for local platform "
            f"{local_image.platform}"
        )
    return platform_matches[0]


def remote_config_digest(image: str, local_image: LocalDockerImage) -> str:
    """Resolve the remote config digest matching the local image's platform."""

    manifest = inspect_remote_manifest(image)
    if manifest is None:
        raise VersionManagementError(f"published Docker image does not exist: {image}")

    if "config" in manifest:
        return manifest_config_digest(manifest, image)

    descriptor = select_platform_manifest(manifest.get("manifests"), local_image, image)
    digest = descriptor.get("digest")
    if not isinstance(digest, str) or not digest:
        raise VersionManagementError(
            f"published image {image} has an invalid manifest descriptor for {local_image.platform}"
        )

    platform_image = f"{repository_from_image_reference(image)}@{digest}"
    platform_manifest = inspect_remote_manifest(platform_image)
    if platform_manifest is None:
        raise VersionManagementError(
            f"published platform manifest does not exist: {platform_image}"
        )
    return manifest_config_digest(platform_manifest, platform_image)


def assert_image_matches_registry(project: LambdaProject, registry: str) -> None:
    """Check that the current locally built lambda image matches its remote tag."""

    registry = registry.rstrip("/")
    if not registry:
        raise VersionManagementError("registry must not be empty")

    image = f"{registry}/{project.image}"
    local_image = inspect_local_docker_image(image)
    published_digest = remote_config_digest(image, local_image)
    if local_image.image_id != published_digest:
        raise VersionManagementError(
            f"local and published Docker images differ: {image}\n"
            f"  local:     {local_image.image_id}\n"
            f"  published: {published_digest}"
        )

    print(
        f"Local and published Docker images match: {image} "
        f"({local_image.image_id}, {local_image.platform})"
    )


def assert_images_match_registry(selected_projects: Sequence[LambdaProject], registry: str) -> None:
    """Compare every selected image and report all mismatches and lookup failures."""

    errors = []
    for project in selected_projects:
        try:
            assert_image_matches_registry(project, registry)
        except VersionManagementError as error:
            errors.append(str(error))

    if errors:
        details = "\n\n".join(errors)
        raise VersionManagementError(f"{len(errors)} lambda image comparison(s) failed:\n{details}")


def confirm_publish(
    selected_projects: Sequence[LambdaProject],
    registry: str,
    assume_yes: bool,
) -> None:
    """Require confirmation before a push can reassign any existing remote tags."""

    registry = registry.rstrip("/")
    if not registry:
        raise VersionManagementError("registry must not be empty")

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


def create_parser() -> argparse.ArgumentParser:
    """Build the command-line interface used by image and release Make targets."""

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

    return parser


def run(args: argparse.Namespace, repo_root: Path) -> None:
    """Execute the requested publication guard or image comparison command."""

    projects = discover_lambda_projects(repo_root)
    selected = select_projects(projects, args.lambda_selector)

    if args.command == "confirm-publish":
        confirm_publish(selected, args.registry, args.yes)
        return

    if args.command == "check-image-matches-registry":
        assert_images_match_registry(selected, args.registry)


def main(argv: Sequence[str] | None = None, repo_root: Path = REPO_ROOT) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)
    try:
        run(args, repo_root)
    except VersionManagementError as error:
        parser.error(str(error))
    except OperationCancelled as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
