import asyncio
import logging
import random
import re
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from aiohttp import (
    ClientError,
    ClientResponse,
    ClientSession,
    ContentTypeError,
    ServerConnectionError,
)
from cmoncrawl.aggregator.utils import ndjson
from cmoncrawl.common.loggers import all_purpose_logger
from cmoncrawl.common.types import RetrieveResponse

ALLOWED_ERR_FOR_RETRIES = [500, 502, 503, 504]

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
        path_processed = ""
    path_processed = remove_trailing.sub("", path_processed)
    netloc = parsed.netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]

    return f"{netloc}{path_processed}"


async def get_all_CC_indexes(client: ClientSession, cdx_server: str) -> list[str]:
    """
    Get all CC index servers from a given CDX server
    """
    response = await retrieve(
        client=client,
        cdx_server=cdx_server,
        domain=cdx_server,
        params={},
        content_type="application/json",
        max_retry=10,
        sleep_step=1,
    )
    if response.content is None:
        all_purpose_logger.error(
            f"Failed to get CC servers from {cdx_server} after 10 attempts"
        )
        return []
    CC_servers = [js["cdx-api"] for js in response.content]
    return CC_servers


async def retrieve(
    client: ClientSession,
    domain: str,
    cdx_server: str,
    params: dict[str, Any],
    content_type: str,
    max_retry: int,
    sleep_step: int,
    allowed_status_errors: list[int] = ALLOWED_ERR_FOR_RETRIES,
    **args: Any,
):
    def should_retry(retry: int, reason: str, status: int, **args: Any):
        # if logger at least info then report every retry otherwise report every 10 retries
        if all_purpose_logger.level <= logging.DEBUG or retry % 10 == 0:
            all_purpose_logger.error(
                f"Failed to retrieve page of {domain} from {cdx_server} with reason {status}: {reason} retry: {retry + 1}/{max_retry} add_info: {args}"
            )
        if status not in allowed_status_errors:
            return False

        return True

    status = 0
    content = None
    reason: str | None = None
    decode: Callable[[ClientResponse], Awaitable[Any]]

    if content_type == "text/x-ndjson":
        decode = lambda response: response.json(
            content_type=content_type, loads=ndjson.Decoder().decode
        )
    elif content_type == "application/json":
        decode = lambda response: response.json(content_type=content_type)
    else:
        raise ValueError(f"Unknown content type: {content_type}")

    for retry in range(max_retry):
        try:
            all_purpose_logger.debug(
                f"Sending request to {cdx_server} with params: {params}, retry: {retry + 1}/{max_retry}"
            )
            async with client.get(cdx_server, params=params) as response:
                status = response.status
                if not response.ok:
                    reason = str(response.reason) if response.reason else "Unknown"  # type: ignore
                    if not should_retry(retry, reason, status, **args):
                        break
                else:
                    content = await decode(response)
                    all_purpose_logger.info(
                        f"Successfully retrieved page of {domain} from {cdx_server} add_info: {args}"
                    )
                    break

        except (
            ClientError,
            TimeoutError,
            ServerConnectionError,
            ContentTypeError,
        ) as e:
            reason = f"{type(e)} {str(e)}"
            status = 500
            if not should_retry(retry, reason, status, **args):
                break

        await asyncio.sleep(random.randint(0, (retry + 1) * sleep_step))
    return RetrieveResponse(status, content, reason)
