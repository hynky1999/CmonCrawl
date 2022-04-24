from __future__ import annotations
from collections import deque
from dataclasses import dataclass

from Aggregator.errors import PageResponseError
from ndjson_decoder import Decoder
from types import TracebackType
from typing import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    List,
    Dict,
    Tuple,
    Deque,
    Type,
)

from aiohttp import ClientSession
import asyncio


@dataclass
class Domain_Crawl:
    domain: str
    crawl: str
    page: int
    max_page: int


@dataclass
class Domain_Record:
    filename: str
    url: str
    offset: int
    length: int


class DomainIndexer(AsyncIterable[Domain_Record]):
    def __init__(
        self,
        domains: List[str],
        cc_indexes_server: str = "http://index.commoncrawl.org/collinfo.json",
        cc_servers: List[str] | None = None,
    ) -> None:
        self.domains = domains
        self.cc_indexes_server = cc_indexes_server
        self.cc_servers = cc_servers

    async def aopen(self) -> DomainIndexer:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()

        if self.cc_servers is None:
            self.cc_servers = await self.get_all_CC_indexes(
                self.client, self.cc_indexes_server
            )
        return self

    async def __aenter__(self) -> DomainIndexer:
        return await self.aopen()

    @staticmethod
    async def get_number_of_pages(
        client: ClientSession,
        cdx_server: str,
        url: str,
        page_size: int | None = None,
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
            r_json = await response.json(content_type="text/x-ndjson")
            return r_json.get("pages", 0), r_json.get("pageSize", 0)

    @staticmethod
    async def get_captured_responses(
        client: ClientSession, cdx_server: str, url: str, page: int = 0
    ) -> List[Domain_Record]:
        params: Dict[str, str | int] = {
            "output": "json",
            "matchType": "domain",
            "page": page,
            "url": url,
        }

        async with client.get(cdx_server, params=params) as response:
            if not response.ok:
                raise PageResponseError(cdx_server, url, page)
            r_json = await response.json(
                content_type="text/x-ndjson", loads=Decoder().decode
            )
            domain_records = [
                Domain_Record(
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

    def __aiter__(self):
        # This will never happend but it cannot deduce it
        if self.cc_servers is None:
            self.cc_servers = []

        return DomainIndexerIterator(self.client, self.domains, self.cc_servers)

    async def aclose(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> DomainIndexer:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> DomainIndexer:
        return await self.aclose(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)


class DomainIndexerIterator(AsyncIterator[Domain_Record]):
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
        self.__domain_records: List[Domain_Record] = []
        self.__prefetch_queue: Deque[Awaitable[List[Domain_Record]]] = deque([])
        self.__crawls_remaining = self.__init_crawls_queue(domains, CC_files)

    def __init_crawls_queue(
        self, domains: List[str], CC_files: List[str]
    ) -> Deque[Domain_Crawl]:
        return Deque(
            [
                Domain_Crawl(crawl=crawl, domain=domain, page=0, max_page=0)
                for crawl in CC_files
                for domain in domains
            ]
        )

    async def __prefetch_next_crawl(self):
        if len(self.__crawls_remaining) == 0:
            return

        next_crawl = self.__crawls_remaining.popleft()
        pages, _ = await DomainIndexer.get_number_of_pages(
            self.__client, next_crawl.crawl, next_crawl.domain
        )
        for i in range(pages):
            self.__prefetch_queue.append(
                asyncio.create_task(
                    DomainIndexer.get_captured_responses(
                        self.__client,
                        next_crawl.crawl,
                        next_crawl.domain,
                        page=i,
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
                    DomainIndexer.get_captured_responses(
                        self.__client,
                        err.CC_server,
                        err.domain,
                        page=err.page,
                    )
                )
            )
        return 0

    async def __anext__(self) -> Domain_Record:
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
