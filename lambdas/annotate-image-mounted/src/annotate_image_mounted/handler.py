"""
A lambda which extracts basic image characteristics and stores them as xattrs.
"""

__author__ = "Lukasz Opiola, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Final, TypedDict

import numpy
import scipy.cluster.vq
import webcolors
import xattr
from numpy.typing import NDArray
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    mounted_file_path,
    per_job,
)
from PIL import Image, ImageFile, UnidentifiedImageError


##===================================================================
## Lambda configuration
##===================================================================


MAX_CLUSTER_COUNT: Final[int] = 5
RESIZE_WIDTH: Final[int] = 100

COLOUR_NAMES_TO_HEX: Final[dict[str, str]] = {
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

type PILImage = Image.Image | ImageFile.ImageFile
type RGB = tuple[int, int, int]
type ImageProperties = dict[str, int | str]


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    file: AtmFile


type JobResult = None


##===================================================================
## Lambda implementation
##===================================================================


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], _ctx: JobContext[AtmObject]) -> JobResult:
    file = job.args["file"]
    if file["type"] != "REG":
        return None

    file_path = mounted_file_path(file["fileId"])
    try:
        with Image.open(file_path) as image:
            image_in_rgb = _ensure_image_in_rgb_format(image)
            image_properties = _infer_image_properties(image_in_rgb)
    except (OSError, UnidentifiedImageError):
        return None

    _save_properties_as_xattrs(file_path, image_properties)
    return None


def _ensure_image_in_rgb_format(image: PILImage) -> PILImage:
    if image.mode != "RGB":
        return image.convert("RGB")
    return image


def _infer_image_properties(image: PILImage) -> ImageProperties:
    width, height = image.size
    orientation = "vertical" if height > width else "horizontal"
    return {
        "width": width,
        "height": height,
        "orientation": orientation,
        "average_colour": _calc_average_image_colour(image),
        "dominant_colour": _calc_dominant_image_colour(image),
    }


def _save_properties_as_xattrs(file_path: str, properties: ImageProperties) -> None:
    try:
        file_xattrs = xattr.xattr(file_path)
        for attr_name, attr_val in properties.items():
            file_xattrs.set(attr_name, str(attr_val).encode())
    except Exception as ex:
        raise JobException(f"Failed to set xattrs due to: {ex}") from ex


def _calc_average_image_colour(image: PILImage) -> str:
    histogram = image.histogram()
    r, g, b = histogram[0:256], histogram[256 : 256 * 2], histogram[256 * 2 : 256 * 3]
    return _rgb_to_closest_colour_name(
        (_safe_average(r), _safe_average(g), _safe_average(b))
    )


def _safe_average(channel: list[int]) -> int:
    total_weight = sum(channel)
    if not total_weight:
        return 0
    return int(sum(i * weight for i, weight in enumerate(channel)) / total_weight)


def _calc_dominant_image_colour(image: PILImage) -> str:
    width, height = image.size
    image = image.resize((RESIZE_WIDTH, int(RESIZE_WIDTH * height / width)))

    pixels = numpy.asarray(image, dtype=float)
    image_height, image_width, channels = pixels.shape
    pixels = pixels.reshape(image_height * image_width, channels)

    unique_colours = numpy.unique(pixels, axis=0)
    cluster_count = min(MAX_CLUSTER_COUNT, len(unique_colours))

    counts, codes = _perform_clustering(pixels, cluster_count)
    dominant_index = numpy.argmax(counts)
    r, g, b = codes[dominant_index]
    return _rgb_to_closest_colour_name((int(r), int(g), int(b)))


def _perform_clustering(
    pixels: NDArray[numpy.float64], cluster_count: int
) -> tuple[NDArray[numpy.int64], NDArray[numpy.float64]]:
    codes, _ = scipy.cluster.vq.kmeans(pixels, cluster_count)
    vecs, _ = scipy.cluster.vq.vq(pixels, codes)
    counts, _ = numpy.histogram(vecs, len(codes))
    return counts, codes


def _rgb_to_closest_colour_name(rgb_triplet: RGB) -> str:
    closest_colour = min(
        COLOUR_NAMES_TO_HEX.items(),
        key=lambda item: _euclidean_distance(webcolors.hex_to_rgb(item[1]), rgb_triplet),
    )
    return closest_colour[0]


def _euclidean_distance(c1: RGB, c2: RGB) -> int:
    return sum((a - b) ** 2 for a, b in zip(c1, c2, strict=True))
