from datetime import datetime
import logging
from typing import List
from index_query import DomainRecord, IndexAggregator
import unittest

logging.basicConfig(level=logging.DEBUG)


class TestIndexerAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.CC_SERVERS = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
        self.di = await IndexAggregator(
            ["idnes.cz"], cc_servers=self.CC_SERVERS, max_retry=50
        ).aopen()
        self.client = self.di.client

    async def asyncTearDown(self) -> None:
        await self.di.aclose(None, None, None)

    async def test_indexer_num_pages(self):
        num, size = await self.di.get_number_of_pages(
            self.client, self.CC_SERVERS[0], "idnes.cz"
        )
        self.assertEqual(num, 14)
        self.assertEqual(size, 5)

    async def test_indexer_all_CC(self):
        indexes = await self.di.get_all_CC_indexes(
            self.client, self.di.cc_indexes_server
        )
        # I better make this quickly because next Crawl comming soon
        # Since I plan to make add date restrictions this will be easly fixable
        self.assertEqual(len(indexes), 88)

    async def test_caputred_responses(self):
        responses = await self.di.get_captured_responses(
            self.client, self.CC_SERVERS[0], "idnes.cz", page=0
        )
        self.assertEqual(len(responses), 12066)

    async def test_since(self):
        # That is crawl date not published date
        records: List[DomainRecord] = []
        self.di.since = datetime(2022, 1, 21)

        async for record in self.di:
            self.assertGreaterEqual(record.timestamp, self.di.since)
            records.append(record)

        self.assertEqual(len(records), 131149)

    async def test_to(self):
        # That is crawl date not published date
        records: List[DomainRecord] = []
        self.di.to = datetime(2022, 1, 21)

        async for record in self.di:
            self.assertLessEqual(record.timestamp, self.di.to)
            records.append(record)

        self.assertEqual(len(records), 63244)

    async def test_limit(self):
        records: List[DomainRecord] = []
        self.di.limit = 10
        async for record in self.di:
            records.append(record)

        self.assertEqual(len(records), 10)

    async def test_init_queue_since_to(self):
        iterator = self.di.IndexAggregatorIterator(
            self.client,
            self.di.domains,
            [],
            since=datetime(2022, 5, 1),
            to=datetime(2022, 1, 10),
        )
        # Generates only for 2020
        q = iterator.init_crawls_queue(
            self.di.domains,
            self.di.cc_servers
            + [
                "https://index.commoncrawl.org/CC-MAIN-2021-43-index",
                "https://index.commoncrawl.org/CC-MAIN-2022-21-index",
            ],
        )
        self.assertEqual(len(q), 2)

    async def test_iterator_single(self):
        records: List[DomainRecord] = []
        async for record in self.di:
            records.append(record)

        # Checked by alternative client
        self.assertEqual(len(records), 194393)

    async def test_cancel_iterator_tasks(self):
        self.di.limit = 10
        async for record in self.di:
            pass
        await self.di.aclose(None, None, None)
        for iterator in self.di.iterators:
            for task in iterator.prefetch_queue:
                self.assertTrue(task.cancelled() or task.done())


if __name__ == "__main__":
    unittest.main()
