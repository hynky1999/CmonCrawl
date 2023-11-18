from __future__ import annotations

import asyncio
import re
from collections import deque
from datetime import datetime
from types import TracebackType
from typing import (
    AsyncIterable,
    AsyncIterator,
    Deque,
    Dict,
    List,
    Set,
    Type,
)
from cmoncrawl.common.loggers import all_purpose_logger
from cmoncrawl.common.throttling import Throttler
from cmoncrawl.common.types import (
    DomainRecord,
    DomainCrawl,
    MatchType,
)

from aiohttp import (
    ClientSession,
)

from cmoncrawl.aggregator.utils.helpers import get_all_CC_indexes, retrieve
from cmoncrawl.common.loggers import all_purpose_logger
from cmoncrawl.common.types import (
    DomainCrawl,
    DomainRecord,
    MatchType,
)


class IndexAggregator(AsyncIterable[DomainRecord]):
    """
    This class is responsible for aggregating the index files from commoncrawl.
    It is an async context manager which can then be used as an async iterator
    which yields DomainRecord objects, found in the index files of commoncrawl.

    It uses the commoncrawl index server to find the index files.


    Args:
        domains (List[str]): A list of domains to search for.
        cc_indexes_server (str, optional): The commoncrawl index server to use. Defaults to "http://index.commoncrawl.org/collinfo.json".
        match_type (MatchType, optional): Match type for cdx-api. Defaults to None.
        cc_servers (List[str], optional): A list of commoncrawl servers to use. If [], then indexes will be retrieved from the cc_indexes_server. Defaults to [].
        since (datetime, optional): The start date for the search. Defaults to datetime.min.
        to (datetime, optional): The end date for the search. Defaults to datetime.max.
        limit (int, optional): The maximum number of results to return. Defaults to None.
        max_retry (int, optional): The maximum number of retries for a single request. Defaults to 5.
        prefetch_size (int, optional): The number of indexes to fetch concurrently. Defaults to 3.
        sleep_base: float: The base for the exponential backoff time calculation between retries. Defaults to 1.5.

    Examples:
        >>> async with IndexAggregator(["example.com"]) as aggregator:
        >>>     async for domain_record in aggregator:
        >>>         print(domain_record)

    """

    def __init__(
        self,
        domains: List[str],
        cc_indexes_server: str = "http://index.commoncrawl.org/collinfo.json",
        match_type: MatchType | None = None,
        cc_servers: List[str] = [],
        since: datetime = datetime.min,
        to: datetime = datetime.max,
        limit: int | None = None,
        max_retry: int = 5,
        prefetch_size: int = 3,
        sleep_base: float = 1.5,
        max_requests_per_second: int = 20,
    ) -> None:
        self.domains = domains
        self.cc_indexes_server = cc_indexes_server
        self.cc_servers = cc_servers
        self.since = since
        self.to = to
        self.limit = limit
        self.max_retry = max_retry
        self.prefetch_size = prefetch_size
        self.sleep_base = sleep_base
        self.match_type = match_type
        self.iterators: List[IndexAggregator.IndexAggregatorIterator] = []
        self.throttler = Throttler(int(1000 / max_requests_per_second))

    async def aopen(self) -> IndexAggregator:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()

        if len(self.cc_servers) == 0:
            self.cc_servers = await get_all_CC_indexes(
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
            match_type=self.match_type,
            since=self.since,
            to=self.to,
            limit=self.limit,
            max_retry=self.max_retry,
            prefetch_size=self.prefetch_size,
            sleep_base=self.sleep_base,
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
    async def get_number_of_pages(
        client: ClientSession,
        cdx_server: str,
        domain: str,
        match_type: MatchType | None,
        max_retry: int,
        sleep_base: float,
        page_size: int | None = None,
    ) -> int:
        params: Dict[str, str | int] = {
            "showNumPages": "true",
            "output": "json",
            "url": domain,
        }

        if match_type is not None:
            params["matchType"] = match_type.value

        if page_size is not None:
            params["page_size"] = page_size
        response = await retrieve(
            client,
            cdx_server,
            params,
            "text/x-ndjson",
            max_retry=max_retry,
            sleep_base=sleep_base,
            log_additional_info={
                "type": "num_pages",
                "domain": domain,
                "cdx_server": cdx_server,
            },
        )
        pages = response[0].get("pages", 0)

        return pages

    @staticmethod
    async def get_captured_responses(
        client: ClientSession,
        cdx_server: str,
        domain: str,
        match_type: MatchType | None,
        max_retry: int,
        sleep_base: float,
        page: int,
        since: datetime = datetime.min,
        to: datetime = datetime.max,
    ) -> List[DomainRecord]:
        params: Dict[str, str | int] = {
            "output": "json",
            "page": page,
            "url": domain,
            "from": to_timestamp_format(since),
            "to": to_timestamp_format(to),
        }
        if match_type is not None:
            params["matchType"] = match_type.value
        try:
            reponse = await retrieve(
                client,
                cdx_server,
                params,
                "text/x-ndjson",
                max_retry=max_retry,
                sleep_base=sleep_base,
                log_additional_info={
                    "type": "page",
                    "domain": domain,
                    "page": page,
                    "cdx_server": cdx_server,
                },
            )
        except Exception as e:
            all_purpose_logger.error(
                f"Failed to retrieve page {page} for {domain} from {cdx_server} with reason {e}"
            )
            return []

        domain_records = [
            DomainRecord(
                filename=js.get("filename", 0),
                offset=int(js.get("offset", 0)),
                length=int(js.get("length", 0)),
                url=js.get("url", ""),
                encoding=js.get("encoding"),
                digest=js.get("digest", None),
                timestamp=timestamp_to_datetime(js["timestamp"]),
            )
            for js in reponse
        ]
        all_purpose_logger.info(
            f"Found {len(domain_records)} domain records for {domain} on page {page} from {cdx_server}"
        )
        return domain_records

    class IndexAggregatorIterator(AsyncIterator[DomainRecord]):
        def __init__(
            self,
            client: ClientSession,
            domains: List[str],
            CC_files: List[str],
            match_type: MatchType | None,
            since: datetime,
            to: datetime,
            limit: int | None,
            max_retry: int,
            prefetch_size: int,
            sleep_base: float,
        ):
            self.__client = client
            self.__opt_prefetch_size = prefetch_size
            self.__domain_records: Deque[DomainRecord] = deque()
            self.prefetch_queue: Set[asyncio.Task[List[DomainRecord]]] = set()
            self.__since = since
            self.__to = to
            self.__limit = limit
            self.__max_retry = max_retry
            self.__total = 0
            self.__sleep_base = sleep_base
            self.__match_type = match_type

            self.__crawls_remaining = self.init_crawls_queue(domains, CC_files)

        def init_crawls_queue(
            self, domains: List[str], CC_files: List[str]
        ) -> Deque[DomainCrawl]:
            # TODO Be smarter about this, The last -XX determines the week in the year
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
            """
            Prefetch the next index server
            """
            while len(self.__crawls_remaining) > 0:
                next_crawl = self.__crawls_remaining.popleft()

                try:
                    num_pages = await IndexAggregator.get_number_of_pages(
                        self.__client,
                        next_crawl.cdx_server,
                        next_crawl.domain,
                        match_type=self.__match_type,
                        max_retry=self.__max_retry,
                        sleep_base=self.__sleep_base,
                    )
                except Exception as e:
                    all_purpose_logger.error(
                        f"Failed to retrieve number of pages for {next_crawl.domain} from {next_crawl.cdx_server} with reason {e}"
                    )
                    continue
                all_purpose_logger.info(
                    f"Found {num_pages} pages for {next_crawl.domain} from {next_crawl.cdx_server}"
                )

                for i in range(num_pages):
                    dc = DomainCrawl(next_crawl.domain, next_crawl.cdx_server, i)
                    self.prefetch_queue.add(
                        asyncio.create_task(
                            IndexAggregator.get_captured_responses(
                                self.__client,
                                dc.cdx_server,
                                dc.domain,
                                match_type=self.__match_type,
                                page=dc.page,
                                since=self.__since,
                                to=self.__to,
                                max_retry=self.__max_retry,
                                sleep_base=self.__sleep_base,
                            ),
                        )
                    )
                return num_pages
            return 0

        async def __await_next_prefetch(self):
            """
            Gets the next index retry
            """
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
                    response = task.result()
                    self.__domain_records.extend(response)

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


def to_timestamp_format(date: datetime):
    return date.strftime("%Y%m%d%H%M%S")


def crawl_to_year(crawl: str) -> int:
    year = re.search(r"(?:MAIN-)(\d{4})", crawl)
    if year is None:
        raise ValueError(f"Invalid crawl {crawl}")

    return int(year.groups()[0])


def timestamp_to_datetime(timestamp: str) -> datetime:
    return datetime.strptime(timestamp, "%Y%m%d%H%M%S")
