import unittest
from aiohttp import ClientSession
import os
from Processor.Downloader.download import download_url


class TestDownloader(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client: ClientSession = ClientSession()

    async def test_download_url(self):
        params = {}
        params["url"] = "https://crawler-test.com/titles/empty_title"
        response = await download_url(self.client, params)
        self.assertEqual(response, params["response"])
        response = response.replace("\n", "").replace(" ", "")
        self.assertTrue(
            response.startswith(
                "<!DOCTYPEhtml><html><head><title></title><metacontent="
            )
        )

