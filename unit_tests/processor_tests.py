from typing import Any
import unittest
import os
import re
from Processor.Downloader.download import Downloader
from Processor.Router.router import Router
from UserDefineExtractors.aktualne_cz import AktualneExtractor


class TestDownloader(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.downloader: Downloader = await Downloader().aopen()

    async def test_download_url(self):
        params = {}
        length = 30698
        offset = 863866755
        url = "crawl-data/CC-MAIN-2022-05/segments/1642320302715.38/warc/CC-MAIN-20220121010736-20220121040736-00132.warc.gz"
        res = await self.downloader.download_url(url, offset, length, False, params)
        self.assertIsNotNone(re.search("Provozovatelem serveru iDNES.cz je MAFRA", res))

    async def test_digest_verification_sha(self):
        params = {}
        length = 30698
        offset = 863866755
        url = "crawl-data/CC-MAIN-2022-05/segments/1642320302715.38/warc/CC-MAIN-20220121010736-20220121040736-00132.warc.gz"
        res = await self.downloader.download_url(url, offset, length, False, params)
        hash_type = "sha1"
        digest = "5PWKBZGXQFKX4VHAFUMMN34FC76OBXVX"
        self.assertTrue(self.downloader.verify_digest(hash_type, digest, res))

    async def asyncTearDown(self) -> None:
        await self.downloader.aclose(None, None, None)


class RouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        path = os.path.abspath(__file__)
        # python path from this file
        self.router.load_modules(os.path.join(os.path.dirname(path), "test_routes"))
        self.router.register_route("AAA", [r"www.idnes*", r"1111.cz"])
        # Names to to match by NAME in file
        self.router.register_route("BBB", r"seznam.cz")

    def test_router_route_by_name(self):
        params = {}
        c1 = self.router.route("www.idnes.cz", params)
        c2 = self.router.route("www.i.cz", params)
        c3 = self.router.route("seznam.cz", params)
        self.assertEqual(c3, params["proccessor_cls"])
        self.assertEqual(c1.__name__, "a")
        self.assertEqual(c3.__name__, "b")
        self.assertIsNone(c2, None)


class ExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.aktualne_extractor = AktualneExtractor()

    def test_aktualne_article1(self):
        with open("sites/aktualneCZ/article1.html", "r") as f:
            content = f.read()

        pipe_params = {}
        self.aktualne_extractor.extract(content, pipe_params)
        pipe_params_extracted: dict[str, Any] = pipe_params["extracted"]
        self.assertEqual(
            pipe_params_extracted["headline"],
            "Brankář Mazanec bude v Nashvillu další rok",
        )
        self.assertEqual(
            pipe_params_extracted["article"],
"""
Hokejový brankář Marek Mazanec prodloužil smlouvu s Nashvillem.
Marek Mazanec odchytal v dresu Nashvillu zatím 27 zápasů NHL.
Marek Mazanec odchytal v dresu Nashvillu zatím 27 zápasů NHL. | Foto: Reuters
Nashville - Hokejový brankář Marek Mazanec prodloužil smlouvu s Nashvillem. S vedením Predators uzavřel roční dvoucestný kontrakt, který mu v případě působení v NHL zaručuje příjem 575 000 dolarů za sezonu. Na farmě by si vydělal 100 000 dolarů.
Mazancovi končí dvouletá nováčkovská smlouva, kterou podepsal s Nashvillem po předloňském zisku extraligového titulu s Plzní. V první zámořské sezoně odchytal v NHL 25 utkání, v tomto ročníku nastoupil za Predators jen do dvou zápasů. Jinak působil ve farmářském týmu Milwaukee Admirals v AHL.
"""

