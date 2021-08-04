import os
import mimetypes
import json

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import xattr
import magic
from guesslang import Guess
import pandas as pd

validation_mapping = {
    '.py': {
        'content': 'Python',
        'validator': "verify_language"
    },
    '.c': {
        'content': 'C',
        'validator': "verify_language"
    },
    '.csv': {
        'content': 'csv',
        'validator': "validate_csv"
    }
}


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    file_id = args["item"]["file_id"]
    file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
    metadata_key = args["metadata_key"]

    if args["item"]["type"] == "REG":
        file_type = get_mime_content_type(file_path),
        file_name, file_extension = os.path.splitext(args["item"]["name"])

        if metadata_key != "":
            xd = xattr.xattr(file_path)
            xd.set(f"{metadata_key}.mime-type", str.encode(file_type[0]))

        if file_extension in validation_mapping:
            validator_name = validation_mapping[file_extension]["validator"]
            content_match = globals()[validator_name](validation_mapping[file_extension]["content"], file_path)
            inferred_content = infer_content(file_path)

            if metadata_key != "":
                xd = xattr.xattr(file_path)
                xd.set(f"{metadata_key}.content-match", str.encode(str(content_match)))
                xd.set(f"{metadata_key}.inferred-content", str.encode(inferred_content))

            return json.dumps({
                "format": {
                    "file": args["item"]["name"],
                    "fileType": file_type,
                    "contentMatch": content_match,
                    "inferredContent": inferred_content
                }})

        else:
            return json.dumps({
                "format": {
                    "file": args["item"]["name"],
                    "fileType": file_type,
                    "contentMatch": "unsupported file extension for content inferring"
                }})
    return json.dumps({
        "format": {
            "file": args["item"]["name"],
            "fileType": "not a regular file",
        }})


def extension_match_content(file_path, file_extension):
    validator_name = validation_mapping[file_extension]["validator"]
    return globals()[validator_name](file_extension, file_path)


def infer_content(file_path):
    possible_content_types = []
    for extension in validation_mapping:
        validator_name = validation_mapping[extension]["validator"]
        extension_content = validation_mapping[extension]["content"]
        if globals()[validator_name](extension_content, file_path):
            possible_content_types.append(validation_mapping[extension]["content"])
    if len(possible_content_types) >= 2:
        return " or ".join(possible_content_types)
    elif possible_content_types:
        return possible_content_types[0]
    else:
        return "unable to infer content type"


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


def verify_language(expected_language, file_path):
    guess = Guess()
    with open(file_path, 'rb') as file:
        data = file.read()
        language = guess.language_name(data)
    return bool(language == expected_language)


def validate_csv(expected_format, file_path):
    try:
        pd.read_csv(file_path)
        return True
    except:
        return False

