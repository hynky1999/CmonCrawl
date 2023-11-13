import shutil
from datetime import datetime
import json
from pathlib import Path
import unittest
from cmoncrawl.common.loggers import metadata_logger, all_purpose_logger
from cmoncrawl.integrations.download import DownloadOutputFormat, url_download
from cmoncrawl.integrations.extract import (
    extract_from_files,
    ExtractMode,
)
from cmoncrawl.common.types import ExtractConfig
from parameterized import parameterized

from cmoncrawl.integrations.utils import DAOname
from cmoncrawl.processor.connectors.base import ICC_Dao


class Download_files(unittest.IsolatedAsyncioTestCase):
    """
    CLI Testing
    """

    async def asyncSetUp(self) -> None:
        all_purpose_logger.setLevel("DEBUG")
        metadata_logger.setLevel("DEBUG")
        self.base_folder = Path(__file__).parent / "test_extract"
        self.output_folder = self.base_folder / "output"

    async def asyncTearDown(self) -> None:
        # remoev output folder
        if self.output_folder.exists():
            shutil.rmtree(self.output_folder)

    async def test_load_config(self):
        cfg_path = self.base_folder / "cfg.json"
        with open(cfg_path, "r") as f:
            js = json.load(f)
            cfg: ExtractConfig = ExtractConfig.model_validate(js)

        self.assertEqual(cfg.routes[0].extractors[0].name, "test_extractor")

    @parameterized.expand([(DAOname.API,), (DAOname.S3,)])
    async def test_extract_from_records(self, dao: DAOname):
        cfg_path = self.base_folder / "cfg.json"
        with open(cfg_path, "r") as f:
            js = json.load(f)
            cfg: ExtractConfig = ExtractConfig.model_validate(js)
        await extract_from_files(
            config=cfg,
            files=[self.base_folder / "files" / "file.jsonl"],
            output_path=self.output_folder,
            mode=ExtractMode.RECORD,
            date=datetime(2021, 1, 1),
            max_crawls_per_file=10,
            max_directory_size=1,
            url="",
            max_retry=40,
            sleep_base=1,
            download_method=dao,
        )
        output_folder = Path(self.output_folder / "directory_0")
        output_folder.mkdir(parents=True, exist_ok=True)
        with open(output_folder / "0_file.jsonl") as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 5)
        self.assertEqual(
            json.loads(lines[4])["title"],
            '<title data-document-head-keeper="0">Seznam – najdu tam, co neznám</title>',
        )

    async def test_extract_from_html(self):
        cfg_path = self.base_folder / "cfg.json"
        with open(cfg_path, "r") as f:
            js = json.load(f)
            cfg: ExtractConfig = ExtractConfig.model_validate(js)
        await extract_from_files(
            config=cfg,
            files=[self.base_folder / "files" / "file.html"],
            output_path=self.output_folder,
            mode=ExtractMode.HTML,
            date=datetime(2021, 1, 1),
            max_crawls_per_file=1,
            max_directory_size=10,
            url="",
            max_retry=20,
            sleep_base=1.4,
            download_method=None,
        )
        with open(self.output_folder / "directory_0" / "0_file.jsonl") as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(
            json.loads(lines[0])["title"],
            '<title data-document-head-keeper="0">Seznam – najdu tam, co neznám</title>',
        )


class Download_files(unittest.IsolatedAsyncioTestCase):
    """
    CLI Testing
    """

    async def asyncSetUp(self) -> None:
        all_purpose_logger.setLevel("DEBUG")
        metadata_logger.setLevel("DEBUG")
        self.base_folder = Path(__file__).parent / "test_extract"
        self.output_folder = self.base_folder / "output"

    async def asyncTearDown(self) -> None:
        # remoev output folder
        if self.output_folder.exists():
            shutil.rmtree(self.output_folder)

    @parameterized.expand([(DAOname.API,), (DAOname.S3,)])
    async def test_download_records(self, dao: DAOname):
        await url_download(
            url="https://example.com",
            match_type=None,
            output=self.output_folder / "directory_0" / "0_file.jsonl",
            cc_server=None,
            since=datetime(2021, 1, 1),
            to=datetime(2021, 12, 31),
            limit=100,
            max_retry=40,
            sleep_base=1.4,
            mode=DownloadOutputFormat.RECORD,
            max_crawls_per_file=1,
            max_directory_size=10,
            filter_non_200=True,
            encoding="utf-8",
            download_method=dao,
        )
        self.assertTrue((self.output_folder / "downloaded_file.txt").exists())
        with open(self.output_folder / "directory_0" / "0_file.jsonl") as f:
            lines = f.readlines()

        lines = [json.loads(line) for line in lines]
        self.assertEqual(len(lines), 5)
