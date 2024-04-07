import json
import shutil
import unittest
from datetime import datetime
from pathlib import Path

from parameterized import parameterized

from cmoncrawl.common.types import ExtractConfig
from cmoncrawl.integrations.extract import (
    ExtractMode,
    extract_from_files,
    load_config,
)
from cmoncrawl.integrations.utils import DAOname


class ExtractFiles(unittest.IsolatedAsyncioTestCase):
    """
    CLI Testing
    """

    async def asyncSetUp(self) -> None:
        self.base_folder = Path(__file__).parent / "test_extract"
        self.output_folder = self.base_folder / "output"

    async def asyncTearDown(self) -> None:
        # remoev output folder
        if self.output_folder.exists():
            shutil.rmtree(self.output_folder)

    async def test_load_config(self):
        cfg_path = self.base_folder / "cfg.json"
        cfg: ExtractConfig = load_config(cfg_path)

        self.assertEqual(cfg.routes[0].extractors[0].name, "test_extractor")

    async def test_load_config_invalid_json(self):
        cfg_path = self.base_folder / "cfg_invalid.json"
        with self.assertRaises(ValueError):
            load_config(cfg_path)

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
            max_requests_per_second=10,
            url="",
            max_retry=40,
            sleep_base=1.4,
            download_method=dao,
        )
        output_folder = Path(self.output_folder / "directory_0")
        output_folder.mkdir(parents=True, exist_ok=True)
        with open(output_folder / "0_file.jsonl") as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 5)
        titles = [json.loads(line)["title"] for line in lines]
        self.assertIn(
            '<title data-document-head-keeper="0">Seznam – najdu tam, co neznám</title>',
            titles,
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
            max_requests_per_second=10,
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

    # TODO Add test for download
