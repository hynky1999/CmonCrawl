import asyncio
import json
import os
import re
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

from cmoncrawl.common.loggers import metadata_logger
from cmoncrawl.common.types import DomainRecord, PipeMetadata
from cmoncrawl.processor.dao.api import CCAPIGatewayDAO
from cmoncrawl.processor.dao.base import DownloadError, ICC_Dao
from cmoncrawl.processor.dao.s3 import S3Dao
from cmoncrawl.processor.pipeline.downloader import (
    AsyncDownloader,
    Throttler,
    WarcIterator,
)
from cmoncrawl.processor.pipeline.extractor import HTMLExtractor
from cmoncrawl.processor.pipeline.router import Router
from cmoncrawl.processor.pipeline.streamer import (
    StreamerFileHTML,
    StreamerFileJSON,
)


class AsyncDownloaderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.dr = DomainRecord(
            url="www.idnes.cz",
            filename="crawl-data/CC-MAIN-2022-05/segments/1642320302715.38/warc/CC-MAIN-20220121010736-20220121040736-00132.warc.gz",
            length=30698,
            offset=863866755,
            timestamp=datetime.today(),
            encoding="latin-1",
        )

    async def test_download_api(self):
        async with CCAPIGatewayDAO() as connector:
            downloader = AsyncDownloader(dao=connector, max_retry=50)
            res = (await downloader.download(self.dr))[0][0]
        self.assertIsNotNone(re.search("Provozovatelem serveru iDNES.cz je MAFRA", res))

    async def test_download_s3(self):
        async with S3Dao(aws_profile=CONFIG.AWS_PROFILE) as connector:
            downloader = AsyncDownloader(dao=connector, max_retry=50)
            res = (await downloader.download(self.dr))[0][0]
        self.assertIsNotNone(re.search("Provozovatelem serveru iDNES.cz je MAFRA", res))

    async def test_throttler(self):
        async def dummy_download():
            pass

        # Create an instance of the Throttler
        throttler = Throttler(1000)

        # Start time
        start_time = datetime.now()

        # Call the throttled download method three times concurrently
        await asyncio.gather(
            throttler.throttle(dummy_download),
            throttler.throttle(dummy_download),
            throttler.throttle(dummy_download),
        )

        # End time
        end_time = datetime.now()

        # Check if the throttler delayed the execution by at least 2 second
        self.assertTrue((end_time - start_time).total_seconds() >= 2)

    async def test_logging(self):
        # Create a fake client that always fails
        fake_client = AsyncMock(spec=ICC_Dao)
        fake_client.fetch.side_effect = DownloadError("test", 500)

        downloader = AsyncDownloader(
            max_retry=11,
            dao=fake_client,
            sleep_base=0,
        )

        # Create a some domain record
        domain_record = DomainRecord(
            url="http://test.com",
            filename="test",
            offset=0,
            length=0,
            timestamp=datetime.now(),
        )

        # Attempt to download and expect a failure
        with self.assertLogs(metadata_logger, level="ERROR") as mock_logging:
            with self.assertRaises(DownloadError):
                await downloader.download(domain_record)

            self.assertEqual(len(mock_logging.output), 3)


class WarcIteratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_iterate(self):
        file = Path(__file__).parent / "files" / "mini.warc.gz"
        with WarcIterator(file) as warc:
            warc_records = list(await warc.download(None))

        self.assertEqual(len(warc_records), 3)
        self.assertEqual(warc_records[0][1].rec_type, "warcinfo")
        self.assertEqual(warc_records[2][1].rec_type, "response")


class RouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        path = os.path.abspath(__file__)
        # python path from this file
        self.router.load_modules(Path(path).parent / "test_routes")
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


class ExtractorTests(unittest.TestCase):
    def test_encoding(self):
        def create_html():
            return "<html><body><p>test</p></body></html>".encode("latin-1").decode(
                "latin-1"
            )

        def create_non_utf8():
            return bytes([0x81, 0x81, 0x82, 0x83]).decode("latin-1")

        def create_metadata():
            return PipeMetadata(
                domain_record=DomainRecord(
                    filename="",
                    offset=0,
                    length=0,
                    url="",
                ),
            )

        extractor = HTMLExtractor()  # type: ignore
        metadata = create_metadata()
        extractor.encode(create_html(), metadata)
        # By default utf-8 is tried
        self.assertEqual(metadata.encoding, "utf-8")

        # Can be overwriten by setting encoding
        # Revert back to default
        metadata = create_metadata()
        # Set expected encoding
        metadata.domain_record.encoding = "latin-1"
        extractor.encode(create_html(), metadata)
        self.assertEqual(metadata.encoding, "latin-1")

        # By default if everything fails, the default(latin-1) is used
        metadata = create_metadata()
        extractor.encode(create_non_utf8(), metadata)
        self.assertEqual(metadata.encoding, "latin-1")

        # But we can overwrite it by setting raise_on_error
        metadata = create_metadata()
        extractor.raise_on_encoding = True
        metadata.encoding = "latin-1"
        with self.assertRaises(ValueError):
            extractor.encode(create_non_utf8(), metadata)


class OutStreamerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.html_folder = Path(__file__).parent / "test_html"
        self.json_folder = Path(__file__).parent / "test_json"
        self.outstreamer_json = StreamerFileJSON(self.json_folder, 100, 100)
        self.outstreamer_html = StreamerFileHTML(self.html_folder, 5)
        self.metadata = PipeMetadata(
            DomainRecord(filename="", offset=0, length=0, url="")
        )

    async def test_simple_write(self):
        file = await self.outstreamer_json.stream(dict(), self.metadata)
        self.assertTrue(os.path.exists(file))

    async def test_clean_up(self):
        file = await self.outstreamer_json.stream(dict(), self.metadata)
        await self.outstreamer_json.clean_up()
        self.assertFalse(os.path.exists(file))

    async def test_create_directory(self):
        self.outstreamer_json.max_directory_size = 3
        self.outstreamer_json.max_crawls_in_file = 1
        writes = [
            asyncio.create_task(self.outstreamer_json.stream(dict(), self.metadata))
            for _ in range(15)
        ]
        await asyncio.gather(*writes)
        size = len(os.listdir(self.json_folder))
        self.assertEqual(size, 5)

    async def test_create_multi_file(self):
        self.outstreamer_json.max_directory_size = 1
        self.outstreamer_json.max_crawls_in_file = 5

        writes = [
            asyncio.create_task(self.outstreamer_json.stream(dict(), self.metadata))
            for _ in range(5)
        ]
        await asyncio.gather(*writes)
        size = len(os.listdir(self.json_folder))
        self.assertEqual(size, 1)
        num_lines = sum(
            1 for _ in open(self.json_folder / "directory_0" / "0_file.jsonl", "r")
        )
        self.assertEqual(num_lines, 5)

    async def test_check_content_json(self):
        writes = [
            asyncio.create_task(self.outstreamer_json.stream({"num": n}, self.metadata))
            for n in range(2)
        ]
        files = await asyncio.gather(*writes)
        with open(files[0], "r") as f:
            ct = [json.loads(line) for line in f]
        for i in range(2):
            self.assertEqual(ct[i]["num"], i)

    async def asyncTearDown(self) -> None:
        await self.outstreamer_json.clean_up()
        await self.outstreamer_html.clean_up()
