import re
from langdetect import detect


def split_content_into_sections(content):
    splitter_token = "%%%!$$!%%"
    content_items = content.replace("\n\n", splitter_token).split(splitter_token)
    return content_items


def filter_out_small_phrases(content_items):
    passed_items = []
    for c in content_items:
        item = re.sub(r"\s+", " ", c).strip()
        if item.count(" ") < 4:
            continue

        if not item.endswith("."):
            continue

        passed_items.append(item)
    return passed_items


def extract_doc(content):
    content_items = split_content_into_sections(content)
    passed_items = filter_out_small_phrases(content_items)
    doc = " ".join(passed_items)
    if len(doc) < 900:
        return None
    try:
        lang = detect(doc)
    except:
        return None

    if lang != "en":
        return None
    return doc
