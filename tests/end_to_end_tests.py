import asyncio
import shutil
from datetime import datetime
import json
from pathlib import Path
import unittest
from cmoncrawl.integrations.commands import extract_from_files, ExtractMode
from cmoncrawl.common.types import ExtractConfig


class Extract_from_files(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
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
            cfg: ExtractConfig = ExtractConfig.schema(many=False).load(js)

        self.assertEqual(cfg.routes[0].extractors[0].name, "test_extractor")

    async def test_extract_from_records(self):
        cfg_path = self.base_folder / "cfg.json"
        with open(cfg_path, "r") as f:
            js = json.load(f)
            cfg: ExtractConfig = ExtractConfig.schema(many=False).load(js)
        results = await extract_from_files(
            config=cfg,
            files=[self.base_folder / "files" / "file.json"],
            output_path=self.base_folder / "output",
            mode=ExtractMode.RECORD,
            date=datetime(2021, 1, 1),
            debug=False,
            max_crawls_per_file=10,
            max_directory_size=1,
            url="",
        )
        with open(self.output_folder / "directory_0" / "0_file.json") as f:
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
            cfg: ExtractConfig = ExtractConfig.schema(many=False).load(js)
        results = await extract_from_files(
            config=cfg,
            files=[self.base_folder / "files" / "file.html"],
            output_path=self.base_folder / "output",
            mode=ExtractMode.HTML,
            date=datetime(2021, 1, 1),
            debug=False,
            max_crawls_per_file=1,
            max_directory_size=10,
            url="",
        )
        with open(self.output_folder / "directory_0" / "0_file.json") as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(
            json.loads(lines[0])["title"],
            '<title data-document-head-keeper="0">Seznam – najdu tam, co neznám</title>',
        )
