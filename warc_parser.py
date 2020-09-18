from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from langdetect import detect
import sys
from whitelist import load_whitelist


whitelist = load_whitelist()


BANNED_DOMAIN_EXTS = {"ad", "ae", "af", "ag", "al", "am", "ao", "aq", "ar", "at", "aw", "ax", "az", "ba", "bb", "bd",
                      "be", "bf", "bg", "bh", "bi", "bj", "bn", "bo", "bq", "br", "bs", "bt", "bw", "by", "bz", "cc",
                      "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cu", "cv", "cw", "cx", "cy",
                      "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "fi", "fj",
                      "fm", "fo", "fr", "ga", "gd", "ge", "gf", "gh", "gm", "gn", "gp", "gq", "gr", "gt", "gw", "gy",
                      "hk", "hm", "hn", "hr", "ht", "hu", "il", "iq", "ir", "is", "it", "jm", "jo", "jp", "ke", "kg",
                      "kh", "ki", "km", "kn", "kp", "kr", "kw", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt",
                      "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mk", "ml", "mm", "mn", "mo", "mq", "mr",
                      "mt", "mu", "mv", "mw", "mx", "mz", "na", "nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr",
                      "nu", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "ps", "pt", "pw", "py", "qa", "re",
                      "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sh", "si", "sk", "sl", "sm", "sn", "so",
                      "sr", "ss", "st", "su", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk", "tl",
                      "tm", "tn", "to", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uy", "uz", "va", "vc", "ve", "vn",
                      "vu", "wf", "ws", "ye", "yt", "zm", "zw"}


def get_domain_name(url):
    url_parts = url.split("/")
    if len(url_parts) < 3:
        return None
    domain_name = url_parts[2]
    return domain_name


def get_domain_ext(url):
    domain_name = get_domain_name(url)
    if domain_name is None:
        return None

    if "." not in domain_name:
        return None
    ext = domain_name.split(".")[-1]
    return ext


def get_text_selectolax(html):
    tree = BeautifulSoup(html, "lxml")

    body = tree.body
    if body is None:
        return None

    for tag in body.select("script"):
        tag.decompose()

    for tag in body.select("style"):
        tag.decompose()

    text = body.get_text(separator="\n")
    return text


def extract_from_archive(filepath):
    arch_contents = []
    domain_blames = {}

    BLAME_THRESH = 2
    with open(filepath, 'rb') as stream:
        for rec_ix, record in enumerate(ArchiveIterator(stream)):
            sys.stdout.write(f"rec_ix: {rec_ix} items\r")
            record_data = {}
            if record.rec_type == 'response':
                #                 if record.http_headers.get_header("Content-Language") != "en":
                #                     continue
                url = record.rec_headers['WARC-Target-URI']
                ext = get_domain_ext(url)
                if ext in BANNED_DOMAIN_EXTS:
                    continue

                domain_name = get_domain_name(url)
                if domain_name in domain_blames:
                    if domain_blames[domain_name] >= BLAME_THRESH:
                        continue

                record_data["WARC-Record-ID"] = record.rec_headers["WARC-Record-ID"]
                record_data['WARC-Target-URI'] = record.rec_headers['WARC-Target-URI']
                record_data['WARC-Date'] = record.rec_headers['WARC-Date']
                raw_content = record.content_stream().read()

                try:
                    raw_content = raw_content.decode("utf-8").strip()
                except Exception as e:
                    continue
                content = get_text_selectolax(raw_content)

                if content is None:
                    continue

                if len(content) < 500:
                    continue

                #### keep docs if they are in english
                record_data["content"] = content

                if domain_name in whitelist:
                    arch_contents.append(record_data)
                    continue

                # detect other contents
                try:
                    lang = detect(content.replace("\n", " "))
                except:
                    continue

                if lang != "en":
                    if domain_name not in domain_blames:
                        domain_blames[domain_name] = 0
                    domain_blames[domain_name] += 1

                arch_contents.append(record_data)

    return arch_contents, domain_blames