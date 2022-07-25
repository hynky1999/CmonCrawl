from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from datetime import datetime
import re

from Aggregator.App.ndjson_decoder import Decoder
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Deque,
    List,
    Dict,
    Set,
    Tuple,
    Type,
)
from Aggregator.App.utils import all_purpose_logger

from aiohttp import ClientError, ClientSession, ContentTypeError
import asyncio
import random

ALLOWED_ERR_FOR_RETRIES = [500, 502, 503, 504]


@dataclass
class RetrieveResponse:
    status: int
    content: Any
    reason: None | str


@dataclass
class DomainCrawl:
    domain: str = ""
    cdx_server: str = ""
    page: int = 0


@dataclass
class DomainRecord:
    filename: str
    url: str
    offset: int
    length: int
    digest: str | None = None
    encoding: str | None = None
    timestamp: datetime = datetime.now()


class IndexAggregator(AsyncIterable[DomainRecord]):
    def __init__(
        self,
        domains: List[str],
        cc_indexes_server: str = "http://index.commoncrawl.org/collinfo.json",
        cc_servers: List[str] = [],
        since: datetime = datetime.min,
        to: datetime = datetime.max,
        limit: int | None = None,
        max_retry: int = 5,
        prefetch_size: int = 3,
        sleep_step: int = 2,
    ) -> None:
        self.domains = domains
        self.cc_indexes_server = cc_indexes_server
        self.cc_servers = cc_servers
        self.since = since
        self.to = to
        self.limit = limit
        self.max_retry = max_retry
        self.prefetch_size = prefetch_size
        self.sleep_step = sleep_step
        self.iterators: List[IndexAggregator.IndexAggregatorIterator] = []

    async def aopen(self) -> IndexAggregator:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()

        if self.cc_servers is None or len(self.cc_servers) == 0:
            self.cc_servers = await self.get_all_CC_indexes(
                self.client, self.cc_indexes_server
            )
        return self

    async def __aenter__(self) -> IndexAggregator:
        return await self.aopen()

    def __aiter__(self):
        iterator = self.IndexAggregatorIterator(
            self.client,
            self.domains,
            self.cc_servers,
            since=self.since,
            to=self.to,
            limit=self.limit,
            max_retry=self.max_retry,
            prefetch_size=self.prefetch_size,
            sleep_step=self.sleep_step,
        )
        self.iterators.append(iterator)
        return iterator

    async def aclose(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> IndexAggregator:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        for iterator in self.iterators:
            iterator.clean()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> IndexAggregator:
        return await self.aclose(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)

    @staticmethod
    async def __retrieve(
        client: ClientSession,
        domain: str,
        cdx_server: str,
        params: Dict[str, Any],
        content_type: str,
        max_retry: int,
        allowed_status_errors=ALLOWED_ERR_FOR_RETRIES,
        sleep_step: int = 2,
        **args: Any,
    ):
        def should_retry(retry: int, reason: str, status: int, **args):
            all_purpose_logger.error(
                f"Failed to retrieve page of {domain} from {cdx_server} with reason {status}: {reason} retry: {retry + 1}/{max_retry} add_info: {args}"
            )
            if status not in allowed_status_errors:
                return False

            return True

        status = 0
        content = None
        reason = None

        for retry in range(max_retry):
            try:
                async with client.get(cdx_server, params=params) as response:
                    status = response.status
                    if not response.ok:
                        reason = response.reason if response.reason else "Unknown"
                        if not should_retry(retry, reason, status, **args):
                            break
                        else:
                            continue

                    content = await response.json(
                        content_type=content_type, loads=Decoder().decode
                    )
                    all_purpose_logger.info(
                        f"Successfully retrieved page of {domain} from {cdx_server} add_info: {args}"
                    )
                    break
            except ContentTypeError as e:
                all_purpose_logger.error(str(e), exc_info=True)
                break

            except (ClientError, TimeoutError) as e:
                reason = f"{type(e)} {str(e)}"
                if not should_retry(retry, reason, 0, **args):
                    break

            await asyncio.sleep(random.randint(0, (retry + 1) * sleep_step))
        return RetrieveResponse(status, content, reason)

    @staticmethod
    async def get_number_of_pages(
        client: ClientSession,
        cdx_server: str,
        domain: str,
        max_retry: int,
        page_size: int | None = None,
    ):
        params: Dict[str, str | int] = {
            "showNumPages": "true",
            "output": "json",
            "matchType": "domain",
            "url": domain,
        }

        if page_size is not None:
            params["page_size"] = page_size
        response = await IndexAggregator.__retrieve(
            client,
            domain,
            cdx_server,
            params,
            "text/x-ndjson",
            max_retry=max_retry,
            type="num_pages",
        )
        if response.content is not None:
            pages = response.content[0].get("pages", 0)
            page_size = response.content[0].get("pageSize", 0)
            response.content = pages, page_size
            all_purpose_logger.info(f"Got {pages} pages for {domain}")

        return response

    @staticmethod
    async def get_captured_responses(
        client: ClientSession,
        cdx_server: str,
        domain: str,
        max_retry: int,
        page: int,
        since: datetime = datetime.min,
        to: datetime = datetime.max,
    ):
        params: Dict[str, str | int] = {
            "output": "json",
            "matchType": "domain",
            "page": page,
            "url": domain,
            "from": to_timestamp_format(since),
            "to": to_timestamp_format(to),
        }
        reponse = await IndexAggregator.__retrieve(
            client,
            domain,
            cdx_server,
            params,
            "text/x-ndjson",
            max_retry=max_retry,
            page=page,
        )
        if reponse.content is not None:
            reponse.content = [
                DomainRecord(
                    filename=js.get("filename", 0),
                    offset=int(js.get("offset", 0)),
                    length=int(js.get("length", 0)),
                    url=js.get("url", ""),
                    encoding=js.get("encoding"),
                    digest=js.get("digest", None),
                    timestamp=timestamp_to_datetime(js["timestamp"]),
                )
                for js in reponse.content
            ]
        return reponse

    @staticmethod
    async def get_all_CC_indexes(client: ClientSession, cdx_server: str) -> List[str]:
        async with client.get(cdx_server) as response:
            r_json = await response.json(content_type="application/json")
            CC_servers = [js["cdx-api"] for js in r_json]
            return CC_servers

    class IndexAggregatorIterator(AsyncIterator[DomainRecord]):
        def __init__(
            self,
            client: ClientSession,
            domains: List[str],
            CC_files: List[str],
            since: datetime = datetime.min,
            to: datetime = datetime.max,
            limit: int | None = None,
            max_retry: int = 5,
            prefetch_size: int = 4,
            # time sleeping lineary increases with failed attempts
            sleep_step: int = 3,
        ):
            self.__client = client
            self.__opt_prefetch_size = prefetch_size
            self.__domain_records: Deque[DomainRecord] = deque()
            self.prefetch_queue: Set[
                asyncio.Task[Tuple[RetrieveResponse, DomainCrawl, int]]
            ] = set()
            self.__since = since
            self.__to = to
            self.__limit = limit
            self.__max_retry = max_retry
            self.__total = 0
            self.__sleep_step = sleep_step

            self.__crawls_remaining = self.init_crawls_queue(domains, CC_files)

        def init_crawls_queue(
            self, domains: List[str], CC_files: List[str]
        ) -> Deque[DomainCrawl]:
            return deque(
                [
                    DomainCrawl(cdx_server=crawl, domain=domain, page=0)
                    for crawl in CC_files
                    if (
                        crawl_to_year(crawl) >= self.__since.year
                        and crawl_to_year(crawl) <= self.__to.year
                    )
                    for domain in domains
                ]
            )

        async def __prefetch_next_crawl(self) -> int:
            while len(self.__crawls_remaining) > 0:
                next_crawl = self.__crawls_remaining.popleft()

                retr = await IndexAggregator.get_number_of_pages(
                    self.__client,
                    next_crawl.cdx_server,
                    next_crawl.domain,
                    max_retry=self.__max_retry,
                )
                if retr.content is None:
                    # Failed to retrieve it
                    return await self.__prefetch_next_crawl()
                pages = retr.content[0]

                for i in range(pages):
                    dc = DomainCrawl(next_crawl.domain, next_crawl.cdx_server, i)
                    self.prefetch_queue.add(
                        asyncio.create_task(self.__fetch_next_dc(dc, 0))
                    )
                return pages
            return 0

        async def __await_next_prefetch(self):
            # Wait for the next prefetch to finish
            # Don't prefetch if limit is set to avoid overfetching
            while len(self.__crawls_remaining) > 0 and (
                len(self.prefetch_queue) == 0
                or (
                    self.__limit is None
                    and len(self.prefetch_queue) <= self.__opt_prefetch_size
                )
            ):
                await self.__prefetch_next_crawl()

            while len(self.prefetch_queue) > 0 and len(self.__domain_records) == 0:
                done, self.prefetch_queue = await asyncio.wait(
                    self.prefetch_queue, return_when="FIRST_COMPLETED"
                )
                for task in done:
                    response, dc, retry = task.result()

                    if response.content is not None:
                        self.__domain_records.extend(response.content)
                        all_purpose_logger.info(
                            f"Found {len(response.content)} records for {dc.domain} of {dc.cdx_server}"
                        )
                    else:
                        _max_retry = (
                            self.__max_retry
                            if response.status in ALLOWED_ERR_FOR_RETRIES
                            else retry
                        )
                        if (
                            retry < _max_retry
                            and response.status in ALLOWED_ERR_FOR_RETRIES
                        ):
                            all_purpose_logger.info(
                                f"Retrying {dc.domain} of {dc.cdx_server} retry {retry + 1}/{_max_retry}"
                            )
                            self.prefetch_queue.add(
                                asyncio.create_task(self.__fetch_next_dc(dc, retry + 1))
                            )

            # Nothing more to prefetch

        async def __anext__(self) -> DomainRecord:
            # Stop if we fetched everything or reached limit
            if self.__limit is not None and self.__total >= self.__limit:
                self.clean()
                raise StopAsyncIteration

            # Nothing prefetched
            while len(self.__domain_records) == 0:
                await self.__await_next_prefetch()
                if (
                    len(self.prefetch_queue) == 0
                    and len(self.__crawls_remaining) == 0
                    and len(self.__domain_records) == 0
                ):
                    # No more data to fetch
                    self.clean()
                    raise StopAsyncIteration

            result = self.__domain_records.popleft()
            self.__total += 1
            return result

        # Helper functions
        def clean(self):
            for task in self.prefetch_queue:
                task.cancel()

        async def __fetch_next_dc(self, dc: DomainCrawl, retry: int):
            await asyncio.sleep(random.randint(0, self.__sleep_step * (retry)))
            return (
                await IndexAggregator.get_captured_responses(
                    self.__client,
                    dc.cdx_server,
                    dc.domain,
                    page=dc.page,
                    since=self.__since,
                    to=self.__to,
                    max_retry=1,
                ),
                dc,
                retry,
            )


def to_timestamp_format(date: datetime):
    return date.strftime("%Y%m%d%H%M%S")


def crawl_to_year(crawl: str) -> int:
    year = re.search(r"(?:MAIN-)(\d{4})", crawl)
    if year is None:
        raise ValueError(f"Invalid crawl {crawl}")

    return int(year.groups()[0])


def timestamp_to_datetime(timestamp: str) -> datetime:
    return datetime.strptime(timestamp, "%Y%m%d%H%M%S")
