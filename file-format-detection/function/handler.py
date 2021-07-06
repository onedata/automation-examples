import json
import xattr
import magic
from guesslang import Guess

BLOCK_SIZE = 262144

def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    file_id = args["item"]["file_id"]
    file_name = args["item"]["name"]
    file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
    metadata_key = args["metadata_key"]
    file_format = get_file_format(file_path)

    inferred_file_type = infer_file_type(file_name, file_path)

    if metadata_key != "":
        xd = xattr.xattr(file_path)
        xd.set(metadata_key, str.encode(file_format))
    return json.dumps({"format": file_format})


def get_file_format(file_path):
    file_info = magic.from_file(file_path)
    file_format = file_info.split(",")[0]
    file_format_underscore = file_format.replace(" ", "_")
    return file_format_underscore


def infer_file_type(file_name, file_path):
    file_extension_category = get_file_extension_category(file_name)
    if file_extension_category == "code":
        return "Python"
    elif file_extension_category == "data":
        return "Csv"
    else
        return


def get_file_extension_category(file_name):
    return "code"

# def handle(req: bytes):
#     """handle a request to the function
#     Args:
#         req (str): request body
#     """
#     args = json.loads(req)
#
#     file_id = args["item"]["file_id"]
#     file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
#     metadata_key = args["metadata_key"]
#     file_format = get_file_format(file_path)
#     if metadata_key != "":
#         xd = xattr.xattr(file_path)
#         xd.set(metadata_key, str.encode(file_format))
#     return json.dumps({"format": file_format})
#
#
# def get_file_format(file_path):
#     file_info = magic.from_file(file_path)
#     file_format = file_info.split(",")[0]
#     file_format_underscore = file_format.replace(" ", "_")
#     return file_format_underscore

