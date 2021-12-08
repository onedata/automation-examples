import json
import os.path

from ilock import ILock

IGNORE_OUTPUT: str = "> /dev/null 2>&1"

INTERMEDIATE_SLEEP_TIME_SEC: int = 20


def handle(req: bytes) -> str:
    """Converts specified files to log-term types. Deletes all source and intermediate files.
    Args Structure:
        filePath (str): file path to processed

    Returns:
        convertedFiles (object): information about file conversion
    """
    data = json.loads(req)
    results = [process_item(item) for item in data["argsBatch"]]

    return json.dumps({"resultsBatch": results})


def process_item(args):

    try:
        file_path = args["filePath"]
        files_to_delete = []
        convert_log = " not a file "

        if os.path.isfile(file_path):
            filename, file_extension = os.path.splitext(file_path)

            if file_extension in ['.doc', '.docx', '.pptx']:
                # unoconv is based  on libreoffice, therefore it is
                # not capable of parallel executing.
                with ILock('unoconv-lock'):
                    os.system(f"unoconv -f pdf  \'{file_path}\' {IGNORE_OUTPUT}")
                files_to_delete.append(file_path)
                convert_log = f"{file_extension} -> .pdf/A"

            elif file_extension in ['.png', '.tif', '.tiff']:
                os.system(f"convert {file_path} \'{filename}.jpg\' {IGNORE_OUTPUT}")
                files_to_delete.append(file_path)
                convert_log = f"{file_extension} -> .jpg"

            elif file_extension == '.pdf':
                os.system(f"pdf2archive {file_path} \'{file_path}\' {IGNORE_OUTPUT}")
                convert_log = f"{file_extension} -> .pdf/A"

            elif file_extension in ['.mov', '.mkv']:
                os.system(f"ffmpeg -i {file_path} \'{filename}.mp4\' {IGNORE_OUTPUT}")
                files_to_delete.append(file_path)
                convert_log = f"{file_extension} -> .mp4"

            else:
                convert_log = " conversion not needed "

            # Delete source and intermediate files
            for file in files_to_delete:
                os.remove(file)

        return json.dumps({
            "logs": [{
                "convertStatus": convert_log,
                "file": file_path,
                "deletedFiles": files_to_delete
            }]
        })
    except Exception as ex:
        return json.dumps({
            "exception": {
                "status": f"Conversion failed due to error: {str(ex)}"
            }
        })

