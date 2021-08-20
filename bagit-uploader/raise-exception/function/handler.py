import json



def handle(req: bytes) -> str:
    args = json.loads(req)

    item = args["item"]
    exception = {
        "error": "input store should be empty"
    }

    return json.dumps({"exception": exception})
