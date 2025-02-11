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
from typing import Final, List, NamedTuple, Tuple, TypedDict, Union

import numpy
import scipy.cluster
import webcolors
import xattr
from numpy import ndarray
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmObject,
)
from PIL import Image, ImageFile

##===================================================================
## Lambda configuration
##===================================================================


VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"
REST_REQUEST_TIMEOUT: Final[int] = 60
MOUNT_POINT: Final[str] = "/mnt/onedata"

# configuration of the dominant colour calculation procedure
MAX_CLUSTER_COUNT: int = 5
RESIZE_WIDTH: int = 100

PILImage = Union[Image.Image, ImageFile.ImageFile]


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
            image_in_rgb = ensure_image_in_rgb_format(image)
        except IOError:
            return None

        image_properties = infer_image_properties(image_in_rgb)
        try:
            save_properties_as_xattrs(file_path, image_properties)
        except Exception as ex:
            return AtmException(exception=f"Failed to set xattrs due to: {str(ex)}")
    except Exception:
        return AtmException(exception=traceback.format_exc())
    return None


def build_file_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["file"]["fileId"]}'


def ensure_image_in_rgb_format(image: PILImage) -> PILImage:
    if image.mode != "RGB":
        return image.convert("RGB")
    return image


def infer_image_properties(image: PILImage) -> dict:
    width, height = image.size
    orientation = "vertical" if height > width else "horizontal"
    avg_colour_rgb = calc_average_image_colour(image)
    dominant_colour_rgb = calc_dominant_image_colour(image)
    return {
        "width": width,
        "height": height,
        "orientation": orientation,
        "avg_colour_rgb": avg_colour_rgb,
        "dominant_colour_rgb": dominant_colour_rgb,
    }


def save_properties_as_xattrs(file_path: str, properties: dict) -> None:
    file_xattrs = xattr.xattr(file_path)
    for attr_name, attr_val in properties.items():
        file_xattrs.set(attr_name, str.encode(str(attr_val)))


def calc_average_image_colour(image: PILImage) -> str:
    h = image.histogram()
    r, g, b = h[0:256], h[256 : 256 * 2], h[256 * 2 : 256 * 3]
    return rgb_to_closest_colour_name(
        (safe_average(r), safe_average(g), safe_average(b))
    )


def safe_average(channel: List[int]) -> int:
    total_weight = sum(channel)
    return (
        int(sum(i * w for i, w in enumerate(channel)) / total_weight)
        if total_weight
        else 0
    )


def calc_dominant_image_colour(image: PILImage) -> str:
    width, height = image.size
    image = image.resize(
        (RESIZE_WIDTH, int(RESIZE_WIDTH * height / width))
    )  # optional, to reduce time

    ar = numpy.asarray(image, dtype=float)
    h, w, channels = ar.shape
    ar = ar.reshape(h * w, channels)

    unique_colors = numpy.unique(ar, axis=0)  # avoid using too big number of clusters
    num_clusters = min(MAX_CLUSTER_COUNT, len(unique_colors))

    counts, codes = perform_clustering(ar, num_clusters)

    index_max = numpy.argmax(counts)  # find most frequent
    (r, g, b) = codes[index_max]
    return rgb_to_closest_colour_name((r, g, b))


def perform_clustering(ar: ndarray, num_clusters: int) -> Tuple[ndarray, ndarray]:
    codes, _ = scipy.cluster.vq.kmeans(ar, num_clusters)
    vecs, _ = scipy.cluster.vq.vq(ar, codes)  # assign codes
    counts, _ = numpy.histogram(vecs, len(codes))  # count occurrences
    return counts, codes


def rgb_to_closest_colour_name(rgb_triplet: Tuple[int, int, int]) -> str:
    colours = {name: webcolors.name_to_rgb(name) for name in webcolors.names()}
    closest_color = min(
        colours.items(),
        key=lambda item: euclidean_distance(item[1], rgb_triplet),
    )
    return closest_color[0]


def euclidean_distance(c1: Tuple[int, int, int], c2: Tuple[int, int, int]) -> int:
    return sum((a - b) ** 2 for a, b in zip(c1, c2))
