from typing import List
from Aggregator.index_query import DomainRecord, IndexAggregator
import unittest


class TestIndexerAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.CC_SERVERS = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
        self.di = await IndexAggregator(
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
        self.assertEqual(len(indexes), 88)

    async def test_caputred_responses(self):
        responses = await self.di.get_captured_responses(
            self.client, self.CC_SERVERS[0], "idnes.cz", page=0
        )
        self.assertEqual(len(responses), 12066)

    async def test_iterator(self):
        records: List[DomainRecord] = []

        async def dumb_aggregator(dr: DomainRecord):
            records.append(dr)
            return 1

        await self.di.aggregate(dumb_aggregator, max_retries=9999)

        # Checked by alternative client
        self.assertEqual(len(records), 194393)

    async def test_aggregate(self):
        records: List[DomainRecord] = []

        async def dumb_aggregator(dr: DomainRecord):
            records.append(dr)
            return 1

        await self.di.aggregate(dumb_aggregator, max_retries=9999)

        # Checked by alternative client
        self.assertEqual(len(records), 194393)

    async def test_aggregate_full(self):
        async def dumb_aggregator(dr: DomainRecord):
            records.append(dr)
            return 1

        records: List[DomainRecord] = []
        try:
            async with IndexAggregator(
                domains=["idnes.cz"],
                cc_servers=[
                    "https://index.commoncrawl.org/CC-MAIN-2022-21-index",
                    "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
                    "https://index.commoncrawl.org/CC-MAIN-2021-49-index",
                    "https://index.commoncrawl.org/CC-MAIN-2021-43-index",
                    "https://index.commoncrawl.org/CC-MAIN-2021-39-index",
                    "https://index.commoncrawl.org/CC-MAIN-2021-31-index",
                ],
            ) as di:
                await di.aggregate(dumb_aggregator, max_retries=9999)
        except Exception as e:
            print(e)

        # Checked by alternative client
        self.assertEqual(len(records), 194393)


if __name__ == "__main__":
    unittest.main()

