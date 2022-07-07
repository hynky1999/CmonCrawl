from datetime import datetime
import logging
import re
from typing import Any, Dict
from datetime import datetime
from utils import PipeMetadata


def parse_warc_header(warc: str):
    # Default preprocess WIN -> UNIX
    warc = warc.replace("\r\n", "\n")
    warc_header: Dict[str, Any] = {}
    content_type = re.search("(?<=Content-Type: )(.*)", warc)
    if (
        content_type is None
        or content_type.group(0) != "application/http; msgtype=response"
    ):
        logging.warn("Not html content-type or WARC header not found")

    payload_digest = re.search("(?<=WARC-Payload-Digest: )(.*)", warc)
    if payload_digest is not None:
        warc_header["payload_digest"] = payload_digest.group(0).strip()
    block_digest = re.search("(?<=WARC-Block-Digest: )(.*)", warc)
    if block_digest is not None:
        warc_header["block_digest"] = block_digest.group(0).strip()

    return warc_header


def parse_http_header(http: str):
    # Not ideal parsing, but we don't really need all the headers
    http = http.replace("\r\n", "\n")
    http_header: Dict[str, Any] = {}
    date = re.search("(?<=Date: )(.*)", http)
    if date is not None:
        http_header["date"] = datetime.strptime(
            date.group(0), "%a, %d %b %Y %H:%M:%S %Z"
        )

    http_response_code = re.search(r"(?<=HTTP/\d\.\d )(\d+)", http)
    if http_response_code is not None:
        http_header["http_response_code"] = int(http_response_code.group(0).strip())
    http_charset = re.search(r"(?<=charset=)(\S+)", http)
    if http_charset is not None:
        http_header["charset"] = http_charset.group(0).strip().lower()

    return http_header


def parse_warc(warc_str: str, metadata: PipeMetadata):
    splitted = warc_str.split("\r\n\r\n", maxsplit=2)
    warc_h, http_h, html = "", "", ""
    if len(splitted) == 2:
        # Old format without warc header
        http_h, html = splitted
        html = html[:-1]
        # Strip last separator \n
    elif len(splitted) == 3:
        warc_h, http_h, html = splitted
        html = html[:-4]
        # Strip last separator \r\n\r\n
    metadata.warc_header = parse_warc_header(warc_h)
    metadata.http_header = parse_http_header(http_h)
    return html
