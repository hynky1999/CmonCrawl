from contextlib import redirect_stderr
from typing import List
import reddis_helpers
import unittest


import index_query


class TestIndexerAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.CC_SERVERS = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
        self.di = await index_query.DomainIndexer(
            ["idnes.cz"], cc_servers=self.CC_SERVERS
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
        self.assertEqual(len(indexes), 87)

    async def test_caputred_responses(self):
        responses = await self.di.get_captured_responses(
            self.client, self.CC_SERVERS[0], "idnes.cz", page=0
        )
        self.assertEqual(len(responses), 12066)

    async def test_iterator(self):
        recs: List[index_query.Domain_Record] = []
        async for record in self.di:
            recs.append(record)

        # Checked by alternative client
        self.assertEqual(len(recs), 194393)


class TestRedisAynsc(unittest.IsolatedAsyncioTestCase):
    async def SetUp(self) -> None:
        self.redis_conn = reddis_helpers.create_connection("redis://localhost:6379")
        await self.redis_conn.flushall()

    async def test_process_and_forward(self):
        record = index_query.Domain_Record("idnes.cz", 0, 1)
        result = await redirect_helpers.process_and_forward(self.redis_conn, record)
        self.assertTrue(result)
        result = await redirect_helpers.process_and_forward(self.redis_conn, record)
        self.assertFalse(result)
        record = index_query.Domain_Record("seznam.cz", 0, 1)
        result = await redirect_helpers.process_and_forward(self.redis_conn, record)
        self.assertTrue(result)
