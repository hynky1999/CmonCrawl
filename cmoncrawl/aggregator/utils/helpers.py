import re
from urllib.parse import urlparse
from cmoncrawl.common.loggers import all_purpose_logger

ext_sub = re.compile(r"\.html|\.jpg|\.png|\.zip")
multiple_slash = re.compile(r"/")
path_re = re.compile(r"(\/[a-zA-Z0-9_\-]*)*(\/[a-zA-Z0-9\-]*)")
remove_trailing = re.compile(r"[\/\-0-9]+$")


def unify_url_id(url: str):
    parsed = urlparse(url)
    path_processed = ext_sub.sub("", parsed.path)
    path_processed = multiple_slash.sub("/", path_processed)
    path_match = path_re.search(path_processed)
    if path_match:
        path_processed = path_match.group(0)
    else:
        all_purpose_logger.warning(f"No path match for {url}")
        path_processed = ""
    path_processed = remove_trailing.sub("", path_processed)
    netloc = parsed.netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]

    return f"{netloc}{path_processed}"
