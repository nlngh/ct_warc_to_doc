import json


def load_json(filepath):
    with open(filepath, "r") as f:
        data = f.read()
    data = json.loads(data)
    return data


filepath = "configs/domain_whitelist.json"


def load_whitelist():
    whitelist = load_json(filepath)
    whitelist = whitelist["whitelist"]
    whitelist = set(whitelist)
    return whitelist
