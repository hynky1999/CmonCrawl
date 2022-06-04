from datetime import datetime
import re
from turtle import ht
from typing import Any, Dict
from datetime import datetime


def parse_warc_header(warc: str, pipe_params: Dict[str, Any]):
    # Default preprocess WIN -> UNIX
    warc = warc.replace("\r\n", "\n")
    warc_header = {}
    content_type = re.search("(?<=Content-Type: )(.*)", warc)
    if (
        content_type is None
        or content_type.group(0) != "application/http; msgtype=response"
    ):
        raise ValueError("Not html content-type")

    payload_digest = re.search("(?<=WARC-Payload-Digest: )(.*)", warc)
    if payload_digest is not None:
        warc_header["payload_digest"] = payload_digest.group(0).strip()
    block_digest = re.search("(?<=WARC-Block-Digest: )(.*)", warc)
    if block_digest is not None:
        warc_header["block_digest"] = block_digest.group(0).strip()

    pipe_params["warc_header"] = warc_header


def parse_http_header(http: str, pipe_params: Dict[str, Any]):
    # Not ideal parsing, but we don't really need all the headers
    http = http.replace("\r\n", "\n")
    http_header = {}
    date = re.search("(?<=Date: )(.*)", http)
    if date is not None:
        http_header["date"] = datetime.strptime(
            date.group(0), "%a, %d %b %Y %H:%M:%S %Z"
        )

    pipe_params["http_header"] = http_header


def parse_warc(warc_str: str, pipe_params: Dict[str, Any]):
    # TODO make some checking
    with open("out", "wb") as f:
        f.write(warc_str.encode("latin-1"))
    warc_h, http_h, html = map(str.strip, warc_str.split("\r\n\r\n", maxsplit=2))

    parse_warc_header(warc_h, pipe_params)
    parse_http_header(http_h, pipe_params)
    pipe_params["content"] = html

