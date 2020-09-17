import glob
import json
from warc_parser import get_domain_name
from collections import Counter


def load_json(filepath):
    with open(filepath, "r") as f:
        data = f.read()
    data = json.loads(data)
    return data

if __name__ == "__main__":
    files = glob.glob("resources/*.json")

    data = [load_json(f) for f in files]

    urls = [doc['WARC-Target-URI'] for d in data for doc in d["docs"]]

    domain_names = [get_domain_name(url) for url in urls]

    counter = Counter(domain_names)

    domain_names = [i[0] for i in counter.most_common(3000)]

    out= {"whitelist": domain_names}

    with open("configs/domain_whitelist.json", "w") as f:
        f.write(json.dumps(out))