#!/usr/bin/env python3
# pylint: disable=missing-docstring

"""
A utility script which updates lambda checksums in dumped workflow JSON file.

NOTE: This script does not update lambda properties like docker image tag
or arguments/results specification. You have to do it on your own by modifying
workflow JSON dump manually. This util is intended to recalculate lambda
checksum AFTER performing such modification.

Usage example:
./utils/recalculate-workflow-checksums.py bagit-uploader-mounted -t $TOKEN

How it works:
  1. Loads workflow JSON dump from "workflows" directory.
  2. Creates dummy automation inventory in Onezone for uploading/dumping purposes.
  3. For each lambda in JSON dump:
       For each revision of lambda:
         a) Uploads lambda revision to dummy inventory.
         b) Dumps it back to JSON.
         c) Compares checksum of uploaded and dumped lambda revision and updates
            it in workflow JSON dump if necessary.
         d) Removes uploaded lambda revision from dummy inventory.
  4. Removes dummy inventory.
  5. Saves updated workflow JSON dump (only when there were some changes).
"""

__author__ = "Michał Borzęcki"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import argparse
import json
import os
from typing import NamedTuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Args(NamedTuple):
    workflow_name: str
    domain: str
    token: int


class Ctx(NamedTuple):
    workflow_name: str
    api_base_url: str
    api_base_headers: dict


def main() -> None:
    args = parse_args()
    ctx = create_ctx(args)

    workflow_dump = load_workflow_dump(ctx)
    inventory_id = create_dummy_inventory(ctx)

    have_checksums_changed = False
    for lambda_dump in workflow_dump["revision"]["supplementaryAtmLambdas"].values():
        if update_lambda_checksums(ctx, inventory_id, lambda_dump):
            have_checksums_changed = True

    remove_dummy_inventory(ctx, inventory_id)
    if have_checksums_changed:
        save_workflow_dump(ctx, workflow_dump)


def parse_args() -> Args:
    parser = argparse.ArgumentParser(
        prog="recalculate-workflow-checksums",
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
        # Hiding default help and adding custom help argument parser in order
        # to place required named args before optional ones.
        add_help=False,
    )

    parser.add_argument(
        "workflow_name",
        help='Name of dumped workflow. It should correspond to JSON file name in "workflows" directory (without extension).',
    )

    named_required_args = parser.add_argument_group("required named arguments")
    named_required_args.add_argument(
        "--token", "-t", help="Onezone REST access token.", required=True
    )

    named_optional_args = parser.add_argument_group("optional named arguments")
    named_optional_args.add_argument(
        "--help",
        "-h",
        action="help",
        help="Show this help message and exit.",
    )
    named_optional_args.add_argument(
        "--domain",
        "-d",
        help="Domain of Onezone instance which should be used to recalculate checksums. It is optional and fallbacks to default Onezone domain deployed by one-env.",
        default="dev-onezone.default.svc.cluster.local",
    )

    parsed_args = parser.parse_known_args()[0]

    return Args(
        workflow_name=parsed_args.workflow_name,
        domain=parsed_args.domain,
        token=parsed_args.token,
    )


def create_ctx(args: Args) -> Ctx:
    return Ctx(
        workflow_name=args.workflow_name,
        api_base_url=f"https://{args.domain}/api/v3/onezone",
        api_base_headers={
            "x-auth-token": args.token,
            "content-type": "application/json",
        },
    )


def create_dummy_inventory(ctx: Ctx) -> str:
    print("Creating dummy inventory... ", end="")

    create_inventory_response = requests.post(
        f"{ctx.api_base_url}/user/atm_inventories",
        headers=ctx.api_base_headers,
        data=json.dumps({"name": "__inventory_for_dumps"}),
        verify=False,
    )
    create_inventory_response.raise_for_status()

    print("Done.")

    inventory_location = create_inventory_response.headers["Location"]
    inventory_id = inventory_location.split("/")[-1]
    return inventory_id


def remove_dummy_inventory(ctx: Ctx, inventory_id: str) -> None:
    print("Removing dummy inventory... ", end="")

    remove_inventory_response = requests.delete(
        f"{ctx.api_base_url}/user/atm_inventories/{inventory_id}",
        headers=ctx.api_base_headers,
        verify=False,
        timeout=60,
    )
    remove_inventory_response.raise_for_status()

    print("Done.")


def load_workflow_dump(ctx: Ctx) -> dict:
    print(f'Loading "{ctx.workflow_name}" workflow dump from file... ', end="")

    dump_path = get_workflow_dump_path(ctx.workflow_name)
    with open(dump_path, "r") as fd:
        dump = json.load(fd)

    print("Done.")
    return dump


def save_workflow_dump(ctx: Ctx, workflow_dump: dict) -> None:
    print(f'Saving modified "{ctx.workflow_name}" workflow dump to file... ', end="")

    dump_path = get_workflow_dump_path(ctx.workflow_name)
    with open(dump_path, "w") as fd:
        json.dump(workflow_dump, fd, indent=2)
        print("Done.")


def update_lambda_checksums(ctx: Ctx, inventory_id: str, lambda_dump: dict) -> bool:
    have_checksums_changed = False
    for rev_dump in lambda_dump.values():
        if update_lambda_revision_checksum(ctx, inventory_id, rev_dump):
            have_checksums_changed = True

    return have_checksums_changed


def update_lambda_revision_checksum(
    ctx: Ctx, inventory_id: str, lambda_rev_dump: dict
) -> bool:
    lambda_rev_no = int(lambda_rev_dump["revision"]["originalRevisionNumber"])
    revision_content = lambda_rev_dump["revision"]["atmLambdaRevision"]

    def log_progress(message, omit_new_line=False):
        print(
            f'Lambda "{revision_content["name"]}" rev. {lambda_rev_no}: {message}',
            end=("" if omit_new_line else "\n"),
        )

    log_progress("uploading revision... ", True)
    lambda_id = upload_lambda_revision(ctx, inventory_id, lambda_rev_dump)
    print("Done.")

    log_progress("dumping uploaded revision... ", True)
    new_lambda_rev_dump = dump_uploaded_lambda_revision(ctx, lambda_id, lambda_rev_no)
    print("Done.")

    new_checksum = new_lambda_rev_dump["revision"]["atmLambdaRevision"]["checksum"]
    has_checksum_changed = new_checksum != revision_content["checksum"]

    if has_checksum_changed:
        log_progress(f'checksum has changed to "{new_checksum}".')
        revision_content["checksum"] = new_checksum
    else:
        log_progress("checksum has not changed.")

    log_progress("removing uploaded revision... ", True)
    remove_uploaded_lambda_revision(ctx, inventory_id, lambda_id)
    print("Done.")

    return has_checksum_changed


def upload_lambda_revision(ctx: Ctx, inventory_id: str, lambda_rev_dump: dict) -> str:
    revision_to_upload = lambda_rev_dump.copy()
    revision_to_upload["atmInventoryId"] = inventory_id

    create_lambda_response = requests.post(
        f"{ctx.api_base_url}/atm_lambdas",
        headers=ctx.api_base_headers,
        data=json.dumps(revision_to_upload),
        verify=False,
    )
    create_lambda_response.raise_for_status()

    lambda_location = create_lambda_response.headers["Location"]
    lambda_id = lambda_location.split("/")[-1]
    return lambda_id


def dump_uploaded_lambda_revision(ctx: Ctx, lambda_id: str, lambda_rev_no: int) -> dict:
    dump_lambda_response = requests.post(
        f"{ctx.api_base_url}/atm_lambdas/{lambda_id}/dump",
        headers=ctx.api_base_headers,
        data=json.dumps({"includeRevision": lambda_rev_no}),
        verify=False,
    )
    dump_lambda_response.raise_for_status()

    return dump_lambda_response.json()


def remove_uploaded_lambda_revision(
    ctx: Ctx, inventory_id: str, lambda_id: str
) -> None:
    remove_lambda_response = requests.delete(
        f"{ctx.api_base_url}/atm_lambdas/{lambda_id}/atm_inventories/{inventory_id}",
        headers=ctx.api_base_headers,
        verify=False,
    )
    remove_lambda_response.raise_for_status()


def get_workflow_dump_path(workflow_name: str) -> str:
    return f"{os.path.dirname(os.path.realpath(__file__))}/../workflows/{workflow_name}.json"


if __name__ == "__main__":
    main()
