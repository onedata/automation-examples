import json
import random


def handle(req):
    """Randomly fails or returns given input.
    Args:
        req (str): request body
    """
    data = json.loads(req)
    return json.dumps({'resultsBatch': [random_inout(arg) for arg in data['argsBatch']]})


def random_inout(arg):
    if random.randint(1, 3) == 1:
        return {'exception': 'heh'}
    else:
        return arg
