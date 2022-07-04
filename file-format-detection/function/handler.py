import os
import mimetypes
import json
import xattr
import magic
from typing import NamedTuple


class FileFormat(NamedTuple):
    name: str
    mime_type: str
    extensions: list[str]


def handle(req: bytes) -> str:
    """Function returns file formats, based on its name and content. Sets results as metadata.
    Args:
        item (Any-File): file to process
        metadata_key (String): namespace key of metadata entry
    """
    data = json.loads(req)

    results = [process_item(item) for item in data["argsBatch"]]

    return json.dumps({"resultsBatch": results})


def process_item(args):
    file = args["item"]
    metadata_key = args["metadata_key"]

    if file["type"] != "REG":
        return create_failure_result(file, "NOT_A_REGULAR_FILE")

    try:
        file_format = get_file_format(file)
        is_extension_matching_format = check_file_extension_matches_format(
            file["name"], file_format
        )

        if metadata_key != "":
            set_file_format_xattrs(
                file, metadata_key, file_format, is_extension_matching_format
            )

        return create_success_result(file, file_format, is_extension_matching_format)

    except Exception as ex:
        return create_failure_result(file, str(ex))


def create_empty_result(file: dict) -> dict:
    return {"result": {"file-id": file["file_id"], "file-name": file["name"]}}


def create_success_result(
    file: dict, file_format: FileFormat, is_extension_matching_format: bool
) -> dict:
    result = create_empty_result(file)
    result["result"]["detection-passed"] = True
    result["result"]["format"] = {
        "name": file_format.name,
        "mime-type": file_format.mime_type,
        "extensions": file_format.extensions,
    }
    result["result"]["is-extension-matching-format"] = is_extension_matching_format
    return result


def create_failure_result(file: dict, reason: str) -> dict:
    result = create_empty_result(file)
    result["result"]["detection-passed"] = False
    result["result"]["reason"] = reason
    return result


def get_file_path(file: dict) -> str:
    return f"/mnt/onedata/.__onedata__file_id__{file['file_id']}"


def get_file_format(file: dict) -> FileFormat:
    file_path = get_file_path(file)

    format_name = get_file_format_name(file_path)
    mime_type = get_file_mime_type(file_path)
    format_extensions = get_mime_type_extensions(mime_type)

    return FileFormat(
        name=format_name,
        mime_type=mime_type,
        extensions=format_extensions,
    )


def check_file_extension_matches_format(
    file_name: str, file_format: FileFormat
) -> bool:
    used_extension = os.path.splitext(file_name)[1].lower()
    return len(file_format.extensions) == 0 or used_extension in file_format.extensions


def get_file_format_name(file_path: str) -> str:
    return magic.from_file(file_path)


def get_file_mime_type(file_path: str) -> str:
    return magic.from_file(file_path, mime=True)


def get_mime_type_extensions(mime_type: str) -> list[str]:
    return mimetypes.guess_all_extensions(mime_type)


def set_file_format_xattrs(
    file: dict,
    metadata_key: str,
    file_format: FileFormat,
    is_extension_matching_format: bool,
) -> None:
    file_path = get_file_path(file)
    file_xattrs = xattr.xattr(file_path)
    file_xattrs.set(f"{metadata_key}.format-name", str.encode(file_format.name))
    file_xattrs.set(f"{metadata_key}.mime-type", str.encode(file_format.mime_type))
    file_xattrs.set(
        f"{metadata_key}.is-extension-matching-format",
        str.encode(str(is_extension_matching_format)),
    )
