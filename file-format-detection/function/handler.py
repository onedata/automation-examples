import json
import xattr
import magic
from guesslang import Guess
import mimetypes


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    file_id = args["item"]["file_id"]
    file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
    metadata_key = args["metadata_key"]

    guessed_language = guess_language(file_path)
    mime_type = get_mime_type(file_path)
    file_format = get_file_format(file_path)
    valid_csv = is_valid_csv(file_path)
    if metadata_key != "":
        xd = xattr.xattr(file_path)
        xd.set(metadata_key, str.encode(file_format))
    file_info = {
        "mime_type": mime_type,
        "guessed_language": guessed_language
    }
    return json.dumps({"format": file_format})


def get_file_format(file_path):
    file_info = magic.from_file(file_path)
    file_format = file_info.split(",")[0]
    file_format_underscore = file_format.replace(" ", "_")
    return file_format_underscore


def guess_language(file_path):
    guess = Guess()
    with open(file_path, 'r') as file:
        data = file.read()
        language = guess.language_name(data)
        return language


def get_mime_type(file_path):
    mime_type, encoding = mimetypes.guess_type(file_path, strict=True)
    return mime_type

def is_valid_csv(file_path):
    try:
        return True
    except:
        return False

