import unittest
import os
from aiohttp import ClientSession
from Processor.Downloader.download import download_url
from Processor.Router.router import Router


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


class RouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        path = os.path.abspath(__file__)
        # python path from this file
        self.router.load_modules(os.path.join(os.path.dirname(path), "test_routes"))
        self.router.register_route("AAA", [r"www.idnes*", r"1111.cz"])
        self.router.register_route("BBB", r"seznam.cz")

    def test_router_route_by_name(self):
        c1 = self.router.route("www.idnes.cz")
        c2 = self.router.route("www.i.cz")
        c3 = self.router.route("seznam.cz")
        self.assertEqual(c1.__name__, "a")
        self.assertEqual(c3.__name__, "b")
        self.assertIsNone(c2, None)

