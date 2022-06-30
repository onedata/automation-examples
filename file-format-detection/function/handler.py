import os
import mimetypes
import json
import xattr
import magic
from typing import List


def handle(req: bytes) -> str:
    """Function returns file formats, based on its name and content. Sets results as metadata.
    Args:
        item (Any-File): file to process
        metadata_key (String): namespace key of metadata entry
    """
    data = json.loads(req)

    results = [process_item(item) for item in data['argsBatch']]

    return json.dumps({"resultsBatch": results})


def process_item(args):
    try:
        if args["item"]["type"] != "REG":
            return {
                "format": {
                    "file": file_name,
                    "detection-passed": False,
                    "reason": "NOT_A_REGULAR_FILE"
                }
            }

        file_id = args["item"]["file_id"]
        file_name = args["item"]["name"]
        file_path = f"/mnt/onedata/.__onedata__file_id__{file_id}"
        metadata_key = args["metadata_key"]

        format_name = get_file_format_name(file_path)
        mime_type = get_file_mime_type(file_path)
        format_extensions = get_mime_type_extensions(mime_type)
        _, used_extension = os.path.splitext(file_name)
        used_extension_matches_format = len(format_extensions) == 0 or \
                                        used_extension in format_extensions;

        if metadata_key != "":
            file_xattrs = xattr.xattr(file_path)
            file_xattrs.set(f"{metadata_key}.format-name", str.encode(format_name))
            file_xattrs.set(f"{metadata_key}.mime-type", str.encode(mime_type))
            file_xattrs.set(f"{metadata_key}.used-extension-matches-format",
                            str.encode(str(used_extension_matches_format)))


        return {
            "format": {
                "file": file_name,
                "detection-passed": True,
                "name": format_name,
                "extensions": format_extensions,
                "mime-type": mime_type,
                "used-extension-matches-format": used_extension_matches_format
            }
        }

    except Exception as ex:
        return {
            "format": {
                "file": file_name,
                "detection-passed": False,
                "reason": str(ex)
            }
        }


def get_file_format_name(file_path: str) -> str:
    return magic.from_file(file_path)


def get_file_mime_type(file_path: str) -> str:
    return magic.from_file(file_path, mime=True)


def get_mime_type_extensions(mime_type: str) -> List[str]:
    return mimetypes.guess_all_extensions(mime_type)
