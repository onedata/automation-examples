import json
import random


def handle(req):
    """Randomly fails or returns given input.
    Args:
        req (str): request body
    """
    if random.randint(1, 3) == 1:
        return json.dumps({'exception': 'heh'})
    else:
        return req
