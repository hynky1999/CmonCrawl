from datetime import datetime
import re
from typing import Any, Dict
from datetime import datetime
from utils import PipeMetadata, metadata_logger


warc_re = re.compile("^WARC/[0-9.]+")


def parse_warc_header(warc: str, metadata: PipeMetadata):
    # Default preprocess WIN -> UNIX
    warc = warc.replace("\r\n", "\n")
    warc_header: Dict[str, Any] = {}
    content_type = re.search("(?<=Content-Type: )(.*)", warc)
    if (
        content_type is None
        or content_type.group(0) != "application/http; msgtype=response"
    ):
        metadata_logger.warn(
            "Not html content-type or WARC header not found",
            extra={"domain_record": metadata.domain_record},
        )

    payload_digest = re.search("(?<=WARC-Payload-Digest: )(.*)", warc)
    if payload_digest is not None:
        warc_header["payload_digest"] = payload_digest.group(0).strip()
    block_digest = re.search("(?<=WARC-Block-Digest: )(.*)", warc)
    if block_digest is not None:
        warc_header["block_digest"] = block_digest.group(0).strip()

    metadata.warc_header = warc_header
    return warc_header


def parse_http_header(http: str, metadata: PipeMetadata):
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

    metadata.http_header = http_header
    return http_header


def parse_warc(warc_str: str, metadata: PipeMetadata):
    if warc_re.search(warc_str) is None:
        # Old format
        http_h, html = warc_str.split("\r\n\r\n", maxsplit=1)
        # Stripl last /n
        html = html[:-1]
        warc_h = ""
    else:
        warc_h, http_h, html = warc_str.split("\r\n\r\n", maxsplit=2)
        html = html[:-4]
        # Strip last separator \r\n\r\n
    parse_warc_header(warc_h, metadata)
    parse_http_header(http_h, metadata)
    return html
