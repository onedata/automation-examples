import json


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    file_name = args["item"]["name"]

    return json.dumps({"response": f'Hello - {file_name}'})
