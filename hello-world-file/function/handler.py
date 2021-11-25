import json


def handle(req: bytes):
    """Takes files and returns its name with "Hello" prefix.
    Args:
        req (str): request body
    """
    args = json.loads(req)
    results = [process_item(file) for file in args["resultsBatch"]]
    return json.dumps({"resultsBatch": results})


def process_item(file):
    file_name = file["item"]["name"]
    return {"response": f'Hello - {file_name}'}

