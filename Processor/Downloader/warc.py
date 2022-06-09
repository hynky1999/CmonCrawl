from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Dict
from datetime import datetime


@dataclass
class PipeMetadata:
    """
    Metadata for a pipe.
    """

    warc_header: Dict[str, Any]
    http_header: Dict[str, Any]


def parse_warc_header(warc: str):
    # Default preprocess WIN -> UNIX
    warc = warc.replace("\r\n", "\n")
    warc_header: Dict[str, Any] = {}
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

    return http_header


def parse_warc(warc_str: str):
    warc_h, http_h, html = map(str.strip, warc_str.split("\r\n\r\n", maxsplit=2))

    warc_header = parse_warc_header(warc_h)
    http_header = parse_http_header(http_h)
    return html, PipeMetadata(warc_header, http_header)

