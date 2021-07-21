import os
import mimetypes
import json

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import xattr
import magic
from guesslang import Guess


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    file_id = args["item"]["file_id"]
    file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
    metadata_key = args["metadata_key"]

    if metadata_key != "":
        xd = xattr.xattr(file_path)
        xd.set(metadata_key, str.encode("calculated-type"))

    file_info = {
        "mime-filename-type": get_mime_filename_type(file_path),
        "mime-content-type": get_mime_content_type(file_path),
        "guessed-code-language": guess_content_code_language(file_path)
    }
    return json.dumps({"format": file_info})


def get_mime_filename_type(file_path):
    type, encoding = mimetypes.guess_type(file_path, strict=True)
    return type


def get_mime_content_type(file_path):
    return magic.from_file(file_path, mime=True)


def guess_content_code_language(file_path):
    guess = Guess()
    with open(file_path, 'rb') as file:
        data = file.read()
        language = guess.language_name(data)
    return language
