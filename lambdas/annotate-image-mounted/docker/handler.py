"""
A lambda which extracts main characteristics
from image (width, height, orientation, average_colour, dominant_colour)
and sets corresponding xattrs.
"""

__author__ = "Lukasz Opiola"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
import traceback
from typing import Final, Union

import requests
from typing_extensions import NamedTuple, TypedDict

import numpy
import scipy.cluster
import webcolors
import xattr
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmObject,
)
from PIL import Image

##===================================================================
## Lambda configuration
##===================================================================


VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"
REST_REQUEST_TIMEOUT: Final[int] = 60
MOUNT_POINT: Final[str] = "/mnt/onedata"

# configuration of the dominant colour calculation procedure
NUM_CLUSTERS: int = 5
RESIZE_WIDTH: int = 100

colour_names_to_hex = {
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
        return None
    try:
        file_path = build_file_path(job)
        try:
            image = Image.open(file_path)
            image = ensure_image_in_rgb_format(image)
        except IOError:
            return None
        width, height = image.size
        orientation = "vertical" if height > width else "horizontal"

        avg_colour_rgb = calc_average_image_colour(image)
        dominant_colour_rgb = calc_dominant_image_colour(image)

        try:
            file_xattrs = xattr.xattr(file_path)
            file_xattrs.set("width", str.encode(str(width)))
            file_xattrs.set("height", str.encode(str(height)))
            file_xattrs.set("orientation", str.encode(orientation))
            file_xattrs.set("average_colour", str.encode(avg_colour_rgb))
            file_xattrs.set("dominant_colour", str.encode(dominant_colour_rgb))

        except Exception as ex:
            return AtmException(exception=f"Failed to set xattrs due to: {str(ex)}")
    except requests.RequestException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    return None


def build_file_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["file"]["fileId"]}'


def ensure_image_in_rgb_format(image):
    if image.mode != "RGB":
        return image.convert("RGB")
    return image


def safe_average(channel):
    total_weight = sum(channel)
    return (
        int(sum(i * w for i, w in enumerate(channel)) / total_weight)
        if total_weight
        else 0
    )


def calc_average_image_colour(image):
    h = image.histogram()
    r, g, b = h[0:256], h[256 : 256 * 2], h[256 * 2 : 256 * 3]
    return rgb_to_closest_colour_name(
        (safe_average(r), safe_average(g), safe_average(b))
    )


def perform_clustering(ar, num_clusters):
    codes, _ = scipy.cluster.vq.kmeans(ar, num_clusters)
    vecs, _ = scipy.cluster.vq.vq(ar, codes)  # assign codes
    counts, _ = numpy.histogram(vecs, len(codes))  # count occurrences
    return counts, codes


def calc_dominant_image_colour(image):
    width, height = image.size
    image = image.resize(
        (RESIZE_WIDTH, int(RESIZE_WIDTH * height / width))
    )  # optional, to reduce time

    ar = numpy.asarray(image, dtype=float)
    h, w, channels = ar.shape
    ar = ar.reshape(h * w, channels)

    unique_colors = numpy.unique(ar, axis=0)  # avoid using too big number of clusters
    num_clusters = min(NUM_CLUSTERS, len(unique_colors))

    counts, codes = perform_clustering(ar, num_clusters)

    index_max = numpy.argmax(counts)  # find most frequent
    (r, g, b) = codes[index_max]
    return rgb_to_closest_colour_name((r, g, b))


def euclidean_distance(c1, c2):
    return sum((a - b) ** 2 for a, b in zip(c1, c2))


def rgb_to_closest_colour_name(rgb_triplet):
    closest_color = min(
        colour_names_to_hex.items(),
        key=lambda item: euclidean_distance(webcolors.hex_to_rgb(item[1]), rgb_triplet),
    )
    return closest_color[0]
