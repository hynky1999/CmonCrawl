from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from datetime import date

from Aggregator.errors import PageResponseError
from Aggregator.ndjson_decoder import Decoder
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    List,
    Dict,
    Set,
    Tuple,
    Type,
)

from aiohttp import ClientSession, ContentTypeError
import asyncio


@dataclass
class DomainCrawl:
    domain: str
    crawl: str
    page: int = 0
    max_page: int = 0


@dataclass
class DomainRecord:
    filename: str
    url: str
    offset: int
    length: int


class IndexAggregator(AsyncIterable[DomainRecord]):
    def __init__(
        self,
        domains: List[str],
        cc_indexes_server: str = "http://index.commoncrawl.org/collinfo.json",
        cc_servers: List[str] = [],
    ) -> None:
        self.domains = domains
        self.cc_indexes_server = cc_indexes_server
        self.cc_servers = cc_servers

    async def aopen(self) -> IndexAggregator:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()

        if len(self.cc_servers) == 0:
            self.cc_servers = await self.get_all_CC_indexes(
                self.client, self.cc_indexes_server
            )
        return self

    async def __aenter__(self) -> IndexAggregator:
        return await self.aopen()

    def __aiter__(self):
        # This will never happend but it cannot deduce it
        return DomainIndexerIterator(self.client, self.domains, self.cc_servers)

    async def aclose(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> IndexAggregator:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> IndexAggregator:
        return await self.aclose(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)

    @staticmethod
    async def get_number_of_pages(
        client: ClientSession, cdx_server: str, url: str, page_size: int | None = None
    ) -> Tuple[int, int]:
        params: Dict[str, str | int] = {
            "showNumPages": "true",
            "output": "json",
            "matchType": "domain",
            "url": url,
        }

        if page_size is not None:
            params["page_size"] = page_size

        async with client.get(cdx_server, params=params) as response:
            try:
                r_json = await response.json(
                    content_type="text/x-ndjson", loads=Decoder().decode
                )
            except ContentTypeError as e:
                raise PageResponseError(
                    url,
                    status=e.status,
                    reason=e.message,
                    cdx_server=cdx_server,
                    page=0,
                )
            return r_json[0].get("pages", 0), r_json[0].get("pageSize", 0)

    @staticmethod
    async def get_captured_responses(
        client: ClientSession, cdx_server: str, url: str, page: int = 0
    ) -> List[DomainRecord]:
        params: Dict[str, str | int] = {
            "output": "json",
            "matchType": "domain",
            "page": page,
            "url": url,
        }

        async with client.get(cdx_server, params=params) as response:
            if not response.ok:
                raise PageResponseError(
                    url,
                    status=response.status,
                    reason=response.reason,
                    cdx_server=cdx_server,
                    page=page,
                )
            try:
                r_json = await response.json(
                    content_type="text/x-ndjson", loads=Decoder().decode
                )
            except ContentTypeError as e:
                raise PageResponseError(
                    url,
                    status=e.status,
                    reason=e.message,
                    cdx_server=cdx_server,
                    page=page,
                )
            domain_records = [
                DomainRecord(
                    filename=js["filename"],
                    offset=js["offset"],
                    length=js["length"],
                    url=js["url"],
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

    async def aggregate(
        self,
        aggregator_fc: Callable[[DomainRecord], Any],
        max_retries: int = 5,
        since: date = date.min,
        to: date = date.max,
    ):

        tasks: Set[asyncio.Task[int]] = set()
        for index in self.cc_servers:
            print(index)
            index_date = self.index_to_date(index)
            # We cannot check for a day thus ignore it
            since_no_day = date(since.year, 1, 1)
            to_no_day = date(to.year, 1, 1)

            if index_date > to_no_day or index_date < since_no_day:
                continue
            for domain in self.domains:
                tasks.add(
                    asyncio.create_task(
                        SingleSourceAggregator(
                            DomainCrawl(domain=domain, crawl=index),
                            self.client,
                            max_retries,
                        ).aggregate(aggregator_fc),
                        name=f"{domain} {index}",
                    )
                )
        return sum(await asyncio.gather(*tasks))

    def index_to_date(self, index: str) -> date:
        year = index.split("/")[-1].removeprefix("CC-MAIN-").split("-")[0]
        return date(int(year), 1, 1)


class DomainIndexerIterator(AsyncIterator[DomainRecord]):
    def __init__(
        self,
        client: ClientSession,
        domains: List[str],
        CC_files: List[str],
        prefetch_size: int = 4,
    ):
        self.__client = client
        self.__opt_prefetch_size = prefetch_size
        self.__i: int = 0
        self.__domain_records: List[DomainRecord] = []
        self.__prefetch_queue: Deque[Awaitable[List[DomainRecord]]] = deque([])
        self.__crawls_remaining = self.__init_crawls_queue(domains, CC_files)

    def __init_crawls_queue(
        self, domains: List[str], CC_files: List[str]
    ) -> Deque[DomainCrawl]:
        return deque(
            [
                DomainCrawl(crawl=crawl, domain=domain, page=0, max_page=0)
                for crawl in CC_files
                for domain in domains
            ]
        )

    async def __prefetch_next_crawl(self):
        if len(self.__crawls_remaining) == 0:
            return

        next_crawl = self.__crawls_remaining.popleft()
        pages, _ = await IndexAggregator.get_number_of_pages(
            self.__client, next_crawl.crawl, next_crawl.domain
        )
        for i in range(pages):
            self.__prefetch_queue.append(
                asyncio.create_task(
                    IndexAggregator.get_captured_responses(
                        self.__client, next_crawl.crawl, next_crawl.domain, page=i
                    )
                )
            )

    async def __await_next_prefetch(self) -> int:
        # We don't want to prefetch too many
        while (
            len(self.__prefetch_queue) <= self.__opt_prefetch_size
            and len(self.__crawls_remaining) != 0
        ):
            await self.__prefetch_next_crawl()

        if len(self.__prefetch_queue) == 0:
            return 0

        try:
            self.__domain_records = await self.__prefetch_queue.popleft()
            return len(self.__domain_records)

        # On error add to queue again
        # TODO add max retries
        except PageResponseError as err:
            self.__prefetch_queue.append(
                asyncio.create_task(
                    IndexAggregator.get_captured_responses(
                        self.__client, err.cdx_server, err.domain, page=err.page
                    )
                )
            )
        return 0

    async def __anext__(self) -> DomainRecord:
        if (
            self.__i == len(self.__domain_records)
            and len(self.__prefetch_queue) == 0
            and len(self.__crawls_remaining) == 0
        ):
            raise StopAsyncIteration

        if self.__i == len(self.__domain_records):
            new_records = await self.__await_next_prefetch()
            # Reset if we fetched new
            self.__i = 0 if new_records != 0 else self.__i
            return await self.__anext__()

        result = self.__domain_records[self.__i]
        self.__i += 1
        return result


class SingleSourceAggregator:
    def __init__(self, crawl: DomainCrawl, client: ClientSession, max_retries: int = 5):
        self.__crawl = crawl
        self.__client = client
        self.__max_retries = max_retries

    async def aggregate(
        self, aggregator_fn: Callable[[DomainRecord], Coroutine[Any, None, int]]
    ) -> int:
        pages, _ = await IndexAggregator.get_number_of_pages(
            self.__client, self.__crawl.crawl, self.__crawl.domain
        )

        tasks = [
            asyncio.create_task(self.__aggregate_page(aggregator_fn, i, pages))
            for i in range(pages)
        ]
        results = await asyncio.gather(*tasks)
        aggregated_sum = sum(results)
        print(
            f"Finished fetching {self.__crawl.domain} from {self.__crawl.crawl}, aggregated: {aggregated_sum} entries from {pages} pages"
        )
        return aggregated_sum

    async def __aggregate_page(
        self,
        aggregator_fn: Callable[[DomainRecord], Coroutine[Any, None, int]],
        page: int,
        max_page: int,
    ):
        for _ in range(self.__max_retries):
            try:
                responses = await IndexAggregator.get_captured_responses(
                    self.__client, self.__crawl.crawl, self.__crawl.domain, page=page
                )
                break
            except PageResponseError:
                await asyncio.sleep(1)
                continue
        # We failed to get the page
        else:
            print(
                f"Failed to fetch page {page}/{max_page} of {self.__crawl.domain} from {self.__crawl.crawl}"
            )
            return 0

        tasks = [asyncio.create_task(aggregator_fn(r)) for r in responses]
        results = await asyncio.gather(*tasks)
        return sum(results)
