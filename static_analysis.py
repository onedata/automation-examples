#!/usr/bin/env python3

"""
Script performing static code analysis for automation-examples in docker.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in " "LICENSE.txt"

import os
import sys
import argparse
import subprocess


RC_FILE_PATH = "/tmp/rc_file"
DOCKER_AUTOMATION_EXAMPLES_PATH = "/tmp/automation-examples"


def main():
    static_analysis_parser = argparse.ArgumentParser(
        prog="static_analysis",
        formatter_class=argparse.RawTextHelpFormatter,
        description="Perform static code analysis.",
    )

    static_analysis_parser.add_argument(
        "-i",
        "--image",
        default="docker.onedata.org/python_static_analyser:v5",
        help="Docker image",
    )

    static_analysis_args = static_analysis_parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    pylint_cmd = ["pylint", DOCKER_AUTOMATION_EXAMPLES_PATH, "--rcfile", RC_FILE_PATH]
    docker_run_cmd = [
        "docker",
        "run",
        "--rm",
        "-i",
        "-v",
        "{}:{}".format(script_dir, DOCKER_AUTOMATION_EXAMPLES_PATH),
        static_analysis_args.image,
    ] + pylint_cmd

    try:
        subprocess.check_output(docker_run_cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as ex:
        print(ex)
        print("Captured stdout and stderr:")
        print("---------------------------")
        print("")
        print(ex.output.decode())
        print("")
        print("---------------------------")
        sys.exit(ex.returncode)
    else:
        print("OK")


if __name__ == "__main__":
    main()
