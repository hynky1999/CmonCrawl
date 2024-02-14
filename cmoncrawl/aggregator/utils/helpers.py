import asyncio
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import aioboto3
from aiohttp import (
    ClientError,
    ClientSession,
    ContentTypeError,
    ServerConnectionError,
)
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from cmoncrawl.aggregator.utils import ndjson
from cmoncrawl.common.loggers import all_purpose_logger
from cmoncrawl.common.throttling import Throttler

ALLOWED_ERR_FOR_RETRIES = [500, 502, 503, 504]


class DownloadError(Exception):
    def __init__(self, reason: str, status: int):
        self.reason = reason
        self.status = status

    def __str__(self) -> str:
        return f"DownloadError: {self.reason} {self.status}"


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
    try:
        response = await retrieve(
            client=client,
            cdx_server=cdx_server,
            params={},
            content_type="application/json",
            max_retry=10,
            sleep_base=1.4,
            log_additional_info={
                "type": "CC index servers fetch",
                "cdx_server": cdx_server,
            },
        )
    except Exception as e:
        all_purpose_logger.error(
            f"Failed to get CC servers from {cdx_server} with reason: {e}"
        )
        return []
    CC_servers = [js["cdx-api"] for js in response]
    return CC_servers


def log_after_retry(retry_state: RetryCallState):
    retry_num = retry_state.attempt_number - 1
    if all_purpose_logger.level <= logging.DEBUG or retry_num % 5 == 0:
        reason = str(retry_state.outcome.exception())
        cdx_server = retry_state.kwargs.get("cdx_server", "")
        log_additional_info = retry_state.kwargs.get("log_additional_info", {})

        log_message = f"Failed to retrieve from {cdx_server} with reason: '{reason}', retry: {retry_num}"
        if retry_state.next_action:
            log_message += (
                f", waiting {round(retry_state.next_action.sleep, 2)} seconds"
            )

        if log_additional_info:
            log_message += f", additional info: {log_additional_info}"

        all_purpose_logger.error(
            log_message,
        )


async def retrieve(
    client: ClientSession,
    cdx_server: str,
    params: dict[str, Any],
    content_type: str,
    max_retry: int,
    sleep_base: float,
    allowed_status_errors: list[int] = ALLOWED_ERR_FOR_RETRIES,
    log_additional_info: dict[str, Any] = {},  # type: ignore
    throttler: Throttler | None = None,
):
    @retry(
        stop=stop_after_attempt(max_retry + 1),
        wait=wait_random_exponential(multiplier=5, exp_base=sleep_base, max=120),
        retry=retry_if_exception_type(DownloadError),
        reraise=True,
        before_sleep=log_after_retry,
    )
    async def _retrieve_with_throttling(
        client: ClientSession,
        cdx_server: str,
        params: dict[str, Any],
        content_type: str,
        allowed_status_errors: list[int],
        log_additional_info: dict[str, Any],
    ):
        all_purpose_logger.debug(
            f"Sending request to {cdx_server} with params: {params}"
        )

        async def _request():
            try:
                async with client.get(cdx_server, params=params) as response:
                    status = response.status
                    if not response.ok:
                        reason = str(response.reason) if response.reason else "Unknown"  # type: ignore
                        if status in allowed_status_errors:
                            raise DownloadError(reason, status)
                        raise ValueError(
                            f"Failed to download {cdx_server} with status {status} and reason {reason}"
                        )
                    else:
                        if content_type == "text/x-ndjson":
                            content = await response.json(
                                content_type=content_type,
                                loads=ndjson.Decoder().decode,
                            )
                        elif content_type == "application/json":
                            content = await response.json(content_type=content_type)
                        else:
                            raise ValueError(f"Unknown content type: {content_type}")
            except (
                ClientError,
                TimeoutError,
                ServerConnectionError,
                ContentTypeError,
            ) as e:
                reason = f"{type(e)} {str(e)}"
                status = 500
                raise DownloadError(reason, status)

            return content

        if throttler is not None:
            return await throttler.throttle(_request)
        else:
            return await _request()

    return await _retrieve_with_throttling(
        client=client,
        cdx_server=cdx_server,
        params=params,
        content_type=content_type,
        allowed_status_errors=allowed_status_errors,
        log_additional_info=log_additional_info,
    )


def crawl_to_year(crawl: str) -> int:
    year = re.search(r"(?:MAIN-)(\d{4})", crawl)
    if year is None:
        raise ValueError(f"Invalid crawl {crawl}")

    return int(year.groups()[0])


def to_timestamp_format(date: datetime):
    return date.strftime("%Y%m%d%H%M%S")


def timestamp_to_datetime(timestamp: str) -> datetime:
    return datetime.strptime(timestamp, "%Y%m%d%H%M%S")


async def remove_bucket_prefix(session: aioboto3.Session, prefix: str, folder: str):
    # remove all query results
    async with session.client("s3") as s3:
        paginator = s3.get_paginator("list_objects_v2")
        async for result in paginator.paginate(Bucket=prefix, Prefix=folder):
            if "Contents" not in result:
                continue
            for file in result["Contents"]:
                await s3.delete_object(Bucket=prefix, Key=file["Key"])


async def run_athena_query(
    session: aioboto3.Session, query_kwargs: dict[str, Any]
) -> str:
    async with session.client(
        "athena",
        region_name=session.region_name
        if session.region_name is not None
        else "us-east-1",
    ) as athena:
        query_result = await athena.start_query_execution(
            **query_kwargs,
        )
        if query_result["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(f"Athena query failed: {query_result}")

        query_execution_id = query_result["QueryExecutionId"]
        while True:
            response = await athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response["QueryExecution"]["Status"]["State"]
            if status in ["FAILED", "CANCELLED"]:
                raise Exception(f"Athena query failed: {response}")

            if status == "SUCCEEDED":
                break

            await asyncio.sleep(5)
    return query_execution_id
