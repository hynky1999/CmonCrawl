import unittest


import index_query


class TestIndexerAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.CC_SERVERS = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
        self.di = await index_query.DomainIndexer(['indes.cz'], CC_SERVERS=self.CC_SERVERS).aopen()
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
        indexes = await self.di.get_all_CC_indexes(self.client, self.di.CC_INDEXES_SERVER)
        # I better make this quickly because next Crawl comming soon
        # Since I plan to make add date restrictions this will be easly fixable
        self.assertEqual(len(indexes), 87)

    async def test_iterator(self):
        for 
