import json


def handle(req):
    """Returns given data.
    Args:
        req (str): request body
    """
    data = json.loads(req)
    return json.dumps({'resultsBatch': data['argsBatch']})
