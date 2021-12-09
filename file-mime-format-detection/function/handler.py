import mimetypes
import json
import xattr


def handle(req: bytes) -> str:
    """Function returns file format, based only on its name. Sets results as metadata.
    Args:
        file (Any-File): file to process
        metadata_key (String): namespace key of metadata entry
    """
    data = json.loads(req)

    results = [process_item(item) for item in data['argsBatch']]

    return json.dumps({"resultsBatch": results})


def process_item(args):
    try:
        file_id = args["file"]["file_id"]
        file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
        metadata_key = args["metadata_key"]

        if not args["file"]["type"] == "REG":
            raise Exception("Not a regular file")

        file_type = get_mime_filename_type(args["file"]["name"]),
        file_type_str = str(file_type[0])

        if metadata_key != "":
            xd = xattr.xattr(file_path)
            xd.set(f"{metadata_key}.format-type", str.encode(str(file_type_str)))
        return {
            "format": {
                "file": args["file"]["name"],
                "format-extension": str(file_type_str),
            }}

    except Exception as ex:
        return {
            "exception": {
                "file": args["file"]["name"],
                "error": str(ex)
            }}


def get_mime_filename_type(file_path: str) -> str:
    result = mimetypes.guess_type(file_path, strict=True)
    return "".join(result[0])
