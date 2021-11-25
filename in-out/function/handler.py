import json


def handle(req):
    """Returns given data.
    Args:
        req (str): request body
    """
    args = json.loads(req)
    return json.dumps({'resultsBatch': args['argsBatch']})
