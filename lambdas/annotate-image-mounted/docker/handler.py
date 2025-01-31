"""
A lambda which creates a directory structure, expressed using a list of paths,
in the target directory. It will ensure that all provided paths exist
and each path element is a directory, or fail otherwise.
"""

__author__ = "Lukasz Opiola"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
import traceback
from typing import Final, List, Union, cast

import requests
from typing_extensions import NamedTuple, TypedDict

import webcolors
import xattr
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmObject,
)
from PIL import Image
import numpy
import scipy.cluster

##===================================================================
## Lambda configuration
##===================================================================


VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"
REST_REQUEST_TIMEOUT: Final[int] = 60
MOUNT_POINT: Final[str] = "/mnt/onedata"


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    file: AtmFile


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> None:
    results = []
    for job_args in job_batch_request["argsBatch"]:
        results.append(run_job(Job(ctx=job_batch_request["ctx"], args=job_args)))
        heartbeat_callback()


def run_job(job: Job) -> Union[None, AtmException]:
    if job.args["file"]["type"] != "REG":
        return

    try:
        file_path = build_file_path(job)

        try:
            image = Image.open(file_path)
        except IOError:
            return None

        width, height = image.size
        orientation = 'vertical' if height > width else 'horizontal'

        avg_colour_rgb = calc_average_image_colour(image)
        dominant_colour_rgb = calc_dominant_image_colour(image)

        try:
            file_xattrs = xattr.xattr(file_path)
            file_xattrs.set('width', str.encode(str(width)))
            file_xattrs.set('height', str.encode(str(height)))
            file_xattrs.set('orientation', str.encode(orientation))
            file_xattrs.set('average_colour', str.encode(avg_colour_rgb))
            file_xattrs.set('dominant_colour', str.encode(dominant_colour_rgb))

        except Exception as ex:
            raise AtmException(
                exception=f"Failed to set xattrs due to: {str(ex)}"
            )
    except requests.RequestException as ex:
        raise AtmException(exception=str(ex))
    except Exception:
        raise AtmException(exception=traceback.format_exc())
    return None


def build_file_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["file"]["fileId"]}'


def calc_average_image_colour(image):

    h = image.histogram()

    r = h[0:256]
    g = h[256: 256 * 2]
    b = h[256 * 2: 256 * 3]

    return rgb_to_closest_colour_name((
        int(sum(i * w for i, w in enumerate(r)) / sum(r)),
        int(sum(i * w for i, w in enumerate(g)) / sum(g)),
        int(sum(i * w for i, w in enumerate(b)) / sum(b)),
    ))


def calc_dominant_image_colour(image):
    num_clusters = 5
    resize_width = 100

    width, height = image.size
    image = image.resize((resize_width, int(resize_width * height / width)))  # optional, to reduce time

    ar = numpy.asarray(image)
    shape = ar.shape
    ar = ar.reshape(numpy.product(shape[:2]), shape[2]).astype(float)

    codes, dist = scipy.cluster.vq.kmeans(ar, num_clusters)

    vecs, dist = scipy.cluster.vq.vq(ar, codes)         # assign codes
    counts, bins = numpy.histogram(vecs, len(codes))    # count occurrences

    index_max = numpy.argmax(counts)                    # find most frequent
    peak = codes[index_max]
    (r, g, b) = (peak[0], peak[1], peak[2])
    return rgb_to_closest_colour_name((r, g, b))


def rgb_to_closest_colour_name(rgb_triplet):
    min_colours = {}
    for name, hex_val in colour_names_to_hex().items():
        r_c, g_c, b_c = webcolors.hex_to_rgb(hex_val)
        rd = (r_c - rgb_triplet[0]) ** 2
        gd = (g_c - rgb_triplet[1]) ** 2
        bd = (b_c - rgb_triplet[2]) ** 2
        min_colours[(rd + gd + bd)] = name

    return min_colours[min(min_colours.keys())]


def colour_names_to_hex():
    return {
        "black": "#000000",
        "white": "#ffffff",
        "dark gray": "#808080",
        "light gray": "#b0b0b0",
        "red": "#ff0000",
        "orange": "#ffa500",
        "yellow": "#ffff00",
        "green": "#008000",
        "blue": "#0000ff",
        "magenta": "#ff00ff",
        "purple": "#800080",
        "coral": "#ff7f50",
        "maroon": "#800000",
        "navy": "#000080",
        "cyan": "#00ffff",
        "gold": "#ffd700",
        "lime": "#00ff00",
        "jade": "#00a36c",
        "olive": "#808000",
        "pink": "#ffc0cb",
        "brown": "#a52a2a",
        "indigo": "#4b0082",
        "violet": "#ee82ee",
        "turquoise": "#40e0d0",
        "teal": "#008080",
        "salmon": "#fa8072",
        "lavender": "#e6e6fa",
        "plum": "#dda0dd",
        "peach": "#ffe5b4",
        "khaki": "#f0e68c",
        "beige": "#f5f5dc",
        "tan": "#d2b48c",
        "crimson": "#dc143c",
        "sky blue": "#87ceeb",
        "chartreuse": "#7fff00",
        "mint": "#98ff98",
        "rose": "#ff007f",
        "sienna": "#a0522d",
        "mauve": "#e0b0ff",
        "apricot": "#fbceb1",
        "wheat": "#f5deb3",
        "sand": "#c2b280",
    }