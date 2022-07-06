from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from datetime import datetime
import re

from errors import PageResponseError
from ndjson_decoder import Decoder
from types import TracebackType
from typing import Any, AsyncIterable, AsyncIterator, Deque, List, Dict, Tuple, Type

from aiohttp import ClientSession, ContentTypeError
import asyncio
import random

DEFAULT_ENCODING = "latin-1"


@dataclass
class DomainCrawl:
    domain: str = ""
    cdx_server: str = ""
    page: int = 0


@dataclass
class DomainRecords:
    records: deque[DomainRecord]
    origin: DomainCrawl


@dataclass
class DomainRecord:
    filename: str
    url: str
    offset: int
    length: int
    digest: str | None = None
    encoding: str = DEFAULT_ENCODING
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
        **args: Any,
    ):
        async with client.get(cdx_server, params=params) as response:
            if not response.ok:
                raise PageResponseError(
                    domain,
                    status=response.status,
                    reason=response.reason,
                    cdx_server=cdx_server,
                    **args,
                )
            try:
                return await response.json(
                    content_type=content_type, loads=Decoder().decode
                )
            except ContentTypeError as e:
                raise PageResponseError(
                    cdx_server,
                    status=e.status,
                    reason=e.message,
                    cdx_server=cdx_server,
                    **args,
                )

    @staticmethod
    async def get_number_of_pages(
        client: ClientSession,
        cdx_server: str,
        domain: str,
        page_size: int | None = None,
    ) -> Tuple[int, int]:
        params: Dict[str, str | int] = {
            "showNumPages": "true",
            "output": "json",
            "matchType": "domain",
            "url": domain,
        }

        if page_size is not None:
            params["page_size"] = page_size
        r_json = await IndexAggregator.__retrieve(
            client, domain, cdx_server, params, "text/x-ndjson"
        )
        return r_json[0].get("pages", 0), r_json[0].get("pageSize", 0)

    @staticmethod
    async def get_captured_responses(
        client: ClientSession,
        cdx_server: str,
        domain: str,
        page: int = 0,
        since: datetime = datetime.min,
        to: datetime = datetime.max,
        retry: int = 0,
    ) -> List[DomainRecord]:
        params: Dict[str, str | int] = {
            "output": "json",
            "matchType": "domain",
            "page": page,
            "url": domain,
            "from": to_timestamp_format(since),
            "to": to_timestamp_format(to),
        }
        r_json = await IndexAggregator.__retrieve(
            client,
            domain,
            cdx_server,
            params,
            "text/x-ndjson",
            retry=retry,
            page=page,
        )
        domain_records = [
            DomainRecord(
                filename=js.get("filename", 0),
                offset=int(js.get("offset", 0)),
                length=int(js.get("length", 0)),
                url=js.get("url", ""),
                encoding=js.get("encoding", DEFAULT_ENCODING),
                digest=js.get("digest", None),
                timestamp=timestamp_to_datetime(js["timestamp"]),
            )
            for js in r_json
        ]
        return domain_records

    @staticmethod
    async def get_all_CC_indexes(client: ClientSession, cdx_server: str) -> List[str]:
        async with client.get(cdx_server) as response:
            r_json = await response.json(content_type="application/json")
            CC_servers = [js["cdx-api"] for js in r_json]
            return CC_servers

    class IndexAggregatorIterator(AsyncIterator[DomainRecord]):
        # TODO
        # Add better logging to know what we failed to crawl
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
            self.__domain_records: DomainRecords = DomainRecords(deque(), DomainCrawl())
            self.prefetch_queue: Deque[asyncio.Task[DomainRecords]] = deque([])
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
                for i in range(self.__max_retry):
                    try:
                        pages, _ = await IndexAggregator.get_number_of_pages(
                            self.__client, next_crawl.cdx_server, next_crawl.domain
                        )
                        print(
                            f"Succeeded to get number of pages = {pages} of {next_crawl.domain} from {next_crawl.cdx_server}"
                        )
                        break
                    except PageResponseError:
                        # Wait some time before retrying
                        print(
                            f"Failed to retrieve number of pages for {next_crawl.domain} of {next_crawl.cdx_server}"
                        )
                        await asyncio.sleep(
                            random.randint(0, (i + 1) * self.__sleep_step)
                        )
                        pass
                else:
                    # Failed to retrieve it
                    return await self.__prefetch_next_crawl()

                for i in range(pages):
                    self.prefetch_queue.append(
                        asyncio.create_task(
                            self.__fetch_next_page(
                                next_crawl.cdx_server, next_crawl.domain, i, 0
                            )
                        )
                    )
                return pages
            return 0

        async def __await_next_prefetch(self):
            # Wait for the next prefetch to finish
            # Don't prefetch if limit is set to avoid overfetching
            while (self.__limit is None and len(self.prefetch_queue) == 0) or len(
                self.prefetch_queue
            ) <= self.__opt_prefetch_size:
                pages_prefetched = await self.__prefetch_next_crawl()
                if pages_prefetched == 0:
                    break

            while len(self.prefetch_queue) > 0:
                task = self.prefetch_queue.popleft()
                try:
                    self.__domain_records = await task
                    print(
                        f"Suceeded to prefetch page {self.__domain_records.origin.page} of {self.__domain_records.origin.domain} from {self.__domain_records.origin.cdx_server}"
                    )
                    return len(self.__domain_records.records)

                except PageResponseError as err:
                    # Only when Temporaly Unavailable
                    print(
                        f"Failed to retrieve page {err.page} of {err.domain} from {err.cdx_server} with reason {err.reason} retry: {err.retry}/{self.__max_retry}"
                    )
                    if err.retry < self.__max_retry and err.status == 503:
                        await asyncio.sleep(
                            random.randint(0, self.__sleep_step * (1 + err.retry))
                        )
                        self.prefetch_queue.append(
                            asyncio.create_task(
                                self.__fetch_next_page(
                                    err.cdx_server,
                                    err.domain,
                                    err.page,
                                    err.retry + 1,
                                )
                            )
                        )
            # Nothing more to prefetch
            return 0

        async def __anext__(self) -> DomainRecord:
            # Stop if we fetched everything or reached limit
            if self.__limit is not None and self.__total >= self.__limit:
                self.clean()
                raise StopAsyncIteration

            if len(self.__domain_records.records) == 0:
                prefetch_size = await self.__await_next_prefetch()
                if prefetch_size == 0:
                    # No more data to fetch
                    self.clean()
                    raise StopAsyncIteration

            result = self.__domain_records.records.popleft()
            self.__total += 1
            return result

        # Helper functions
        def clean(self):
            for task in self.prefetch_queue:
                task.cancel()

        async def __fetch_next_page(
            self, cdx_server: str, domain: str, page: int, retry: int
        ):
            response = await IndexAggregator.get_captured_responses(
                self.__client,
                cdx_server,
                domain,
                page=page,
                since=self.__since,
                to=self.__to,
                retry=retry,
            )
            return DomainRecords(deque(response), DomainCrawl(domain, cdx_server, page))


def to_timestamp_format(date: datetime):
    return date.strftime("%Y%m%d%H%M%S")


def crawl_to_year(crawl: str) -> int:
    year = re.search(r"(?:MAIN-)(\d{4})", crawl)
    if year is None:
        raise ValueError(f"Invalid crawl {crawl}")

    return int(year.groups()[0])


def timestamp_to_datetime(timestamp: str) -> datetime:
    return datetime.strptime(timestamp, "%Y%m%d%H%M%S")
