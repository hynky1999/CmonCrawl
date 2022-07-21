import asyncio
from pathlib import Path
import unittest
import os
import re
from datetime import datetime
from Downloader.download import Downloader
from Downloader.warc import PipeMetadata
from OutStreamer.stream_to_file import OutStreamerFileDefault
from Router.router import Router
from utils import DomainRecord


class DownloaderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.downloader: Downloader = await Downloader(digest_verification=True).aopen()

    async def test_download_url(self):
        dr = DomainRecord(
            url="www.idnes.cz",
            filename="crawl-data/CC-MAIN-2022-05/segments/1642320302715.38/warc/CC-MAIN-20220121010736-20220121040736-00132.warc.gz",
            length=30698,
            offset=863866755,
            timestamp=datetime.today(),
            encoding="latin-1",
        )
        metadata = PipeMetadata(domain_record=dr)
        res = await self.downloader.download(dr, metadata)
        self.assertIsNotNone(re.search("Provozovatelem serveru iDNES.cz je MAFRA", res))

    async def test_digest_verification_sha(self):
        dr = DomainRecord(
            url="idnes.cz",
            filename="crawl-data/CC-MAIN-2022-05/segments/1642320302715.38/warc/CC-MAIN-20220121010736-20220121040736-00132.warc.gz",
            length=30698,
            offset=863866755,
            timestamp=datetime.today(),
            encoding="latin-1",
        )
        metadata = PipeMetadata(domain_record=dr)
        res = await self.downloader.download(dr, metadata)
        hash_type = "sha1"
        digest = "5PWKBZGXQFKX4VHAFUMMN34FC76OBXVX"
        self.assertTrue(
            self.downloader.verify_digest(
                hash_type, digest, res, metadata.domain_record.encoding
            )
        )

    async def asyncTearDown(self) -> None:
        await self.downloader.aclose(None, None, None)


class RouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        path = os.path.abspath(__file__)
        # python path from this file
        self.router.load_modules(os.path.join(os.path.dirname(path), "test_routes"))
        self.router.register_route("AAA", [r"www.idnes.*", r"1111.cz"])
        # Names to to match by NAME in file
        self.router.register_route("BBB", r"seznam.cz")

    def test_router_route_by_name(self):
        metadata = PipeMetadata(
            domain_record=DomainRecord(
                url="www.idnes.cz",
                timestamp=datetime.today(),
                filename="c",
                offset=0,
                length=0,
            )
        )
        c1 = self.router.route("www.idnes.cz", datetime.today(), metadata)
        try:
            self.router.route("www.i.cz", datetime.today(), metadata)
        except ValueError:
            pass

        c3 = self.router.route("seznam.cz", datetime.today(), metadata)
        self.assertEqual(c1, self.router.modules["AAA"])
        self.assertEqual(c3, self.router.modules["BBB"])


class OutStremaerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.outstreamer_file = OutStreamerFileDefault(origin=Path("./test"))
        self.metadata = PipeMetadata(DomainRecord("", "", 0, 0))

    async def test_simple_write(self):
        file = await self.outstreamer_file.stream(dict(), self.metadata)
        self.assertTrue(os.path.exists(file))

    async def test_clean_up(self):
        file = await self.outstreamer_file.stream(dict(), self.metadata)
        await self.outstreamer_file.clean_up()
        self.assertFalse(os.path.exists(file))

    async def test_create_directory(self):
        self.outstreamer_file.max_directory_size = 3
        writes = [
            asyncio.create_task(self.outstreamer_file.stream(dict(), self.metadata))
            for _ in range(15)
        ]
        await asyncio.gather(*writes)
        size = len(os.listdir("./test"))
        self.assertEqual(size, 5)

    async def asyncTearDown(self) -> None:
        await self.outstreamer_file.clean_up()
