import os
import mimetypes
import json

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import xattr
import magic
from guesslang import Guess
import pandas as pd

validation_mapping: dict = {
    '.py': {
        'content': 'Python',
        'validator': "verify_language"
    },
    '.c': {
        'content': 'C',
        'validator': "verify_language"
    },
    '.cc': {
        'content': 'C++',
        'validator': "verify_language"
    },
    '.csv': {
        'content': 'CSV',
        'validator': "verify_language"
    },
    '.json': {
        'content': 'JSON',
        'validator': "verify_language"
    }
}


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
    file_id = args["item"]["file_id"]
    file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
    metadata_key = args["metadata_key"]

    if args["item"]["type"] == "REG":
        file_type = get_mime_filename_type(args["item"]["name"]),
        file_type_str = str(file_type[0])
        file_name, file_extension = os.path.splitext(args["item"]["name"])

        if metadata_key != "":
            xd = xattr.xattr(file_path)
            xd.set(f"{metadata_key}.format-extension", str.encode(str(file_type_str)))

        if file_extension in validation_mapping:
            validator_name = validation_mapping[file_extension]["validator"]
            content_match = globals()[validator_name](validation_mapping[file_extension]["content"], file_path)
            inferred_content = infer_content(file_path)

            if metadata_key != "":
                xd = xattr.xattr(file_path)
                xd.set(f"{metadata_key}.is-extension-matching-content", str.encode(str(content_match)))
                xd.set(f"{metadata_key}.format-content", str.encode(inferred_content))

            return {
                "format": {
                    "file": args["item"]["name"],
                    "format-extension": file_type[0],
                    "is-extension-matching-content": content_match,
                    "inferredContent": inferred_content
                }}

        else:
            return {
                "format": {
                    "file": args["item"]["name"],
                    "format-extension": file_type[0],
                    "is-extension-matching-content": "unsupported file extension for content inferring"
                }}
    return {
        "format": {
            "file": args["item"]["name"],
            "format-extension": "not a regular file",
        }}


def extension_match_content(file_path: str, file_extension: str) -> str:
    validator_name = validation_mapping[file_extension]["validator"]
    return globals()[validator_name](file_extension, file_path)


def infer_content(file_path: str) -> str:
    possible_content_types = []
    for extension in validation_mapping:
        validator_name = validation_mapping[extension]["validator"]
        extension_content = validation_mapping[extension]["content"]
        if globals()[validator_name](extension_content, file_path):
            possible_content_types.append(validation_mapping[extension]["content"])
    if len(possible_content_types) >= 1:
        return possible_content_types[0]
    else:
        return "unable to infer content type"


def get_mime_filename_type(file_path: str) -> str:
    result = mimetypes.guess_type(file_path, strict=True)
    return "".join(result[0])


def get_mime_content_type(file_path: str) -> str:
    return magic.from_file(file_path, mime=True)


def guess_content_code_language(file_path: str) -> str:
    guess = Guess()
    with open(file_path, 'rb') as file:
        data = file.read()
        language = guess.language_name(data)
    return language


def verify_language(expected_language: str, file_path: str) -> bool:
    guess = Guess()
    with open(file_path, 'rb') as file:
        data = file.read()
        language = guess.language_name(data)
    return bool(language == expected_language)


def validate_csv(expected_format: str, file_path: str) -> bool:
    try:
        pd.read_csv(file_path)
        return True
    except:
        return False
