import hashlib
import zlib
import json
import xattr

SUPPORTED_CHECKSUM_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'adler32']

BLOCK_SIZE = 262144


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    data = json.loads(req)

    results = [process_item(item) for item in data["argsBatch"]]
    return json.dumps({"resultsBatch": results})


def process_item(args):
    if args["item"]["type"] == "REG":
        file_id = args["item"]["file_id"]
        file_path = f'/mnt/onedata/.__onedata__file_id__{file_id}'
        metadata_key = args["metadata_key"]
        algorithm = args["algorithm"]
        assert algorithm in SUPPORTED_CHECKSUM_ALGORITHMS

        with open(file_path, 'rb') as fd:
            checksum = calculate_checksum(fd, algorithm)
            if metadata_key != "":
                xd = xattr.xattr(file_path)
                xd.set(metadata_key, str.encode(checksum))
            return json.dumps({"result": {
                "file_id": file_id,
                "checksum": checksum,
                "algorithm": algorithm
            }})

    return {"result": {}}


def calculate_checksum(fd, algorithm):
    if algorithm == "adler32":
        checksum = 1
        while True:
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum = zlib.adler32(data, checksum)
        return format(checksum, 'x')
    else:
        checksum = getattr(hashlib, algorithm)()
        while True:
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum.update(data)
        return checksum.hexdigest()
