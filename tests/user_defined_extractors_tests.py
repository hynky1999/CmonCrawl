import logging
from pathlib import Path
from datetime import datetime
import json
import unittest
from Processor.App.Downloader.dummy_downloader import DownloaderDummy
from Processor.App.OutStreamer.dummy_streamer import DummyStreamer
from Processor.App.Router.router import Router
from Processor.App.processor_utils import (
    DomainRecord,
    all_purpose_logger,
    metadata_logger,
)
from Processor.App.Pipeline.pipeline import ProcessorPipeline

all_purpose_logger.setLevel(logging.DEBUG)
metadata_logger.setLevel(logging.DEBUG)


SITES_PATH = Path("tests") / "sites"
TRUTH_JSONS = "truth"
TEST_ARTICLES = "test_articles"
FILTER_ARTICLES = "filter_articles"


def parse_extracted_json(extracted_path: Path):
    with open(extracted_path, "r") as f:
        extracted = json.load(f)
    if extracted["publication_date"] is not None:
        extracted["publication_date"] = datetime.fromisoformat(
            extracted["publication_date"]
        )
    return extracted


async def pipeline_wrapper(router: Router, name: Path):
    outstreamer = DummyStreamer()
    files = [path for path in (SITES_PATH / name).glob("*")]
    downloader = DownloaderDummy(files)
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    for _ in files:
        await pipeline.process_domain_record(DomainRecord("", "", 0, 0))

    return outstreamer.data


class ExtractSameTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(Path("Processor/App/DoneExtractors"))
        self.name = ""

    async def test_extract_articles(self):
        if type(self) == ExtractSameTest:
            return

        data = await pipeline_wrapper(self.router, Path(self.name) / TEST_ARTICLES)
        for truth_json in (SITES_PATH / self.name / TRUTH_JSONS).iterdir():
            extracted_truth = parse_extracted_json(truth_json)
            self.assertIn(truth_json.stem + ".html", data.keys())
            extracted = data[truth_json.stem + ".html"]
            self.maxDiff = None
            self.assertDictEqual(extracted, extracted_truth)

    async def test_filter_articles(self):
        if type(self) == ExtractSameTest:
            return

        data = await pipeline_wrapper(self.router, Path(self.name) / FILTER_ARTICLES)
        self.assertListEqual(list(data.keys()), [])


class IrozhlasTests(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("irozhlas_cz", [r".*irozhlas\.cz.*"])
        self.name = "rozhlasCZ"


class SeznamZpravyCZ(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
        self.name = "seznamzpravyCZ"


class NovinkyCZTests(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("novinky_cz_v2", [r".*novinky\.cz.*"])
        self.router.register_route("novinky_cz_v1", [r".*novinky\.cz.*"])
        self.name = "novinkyCZ"


class AktualneCZTests(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("aktualne_cz_v3", [r".*aktualne\.cz.*"])
        self.router.register_route("aktualne_cz_v2", [r".*aktualne\.cz.*"])
        self.router.register_route("aktualne_cz_v1", [r".*aktualne\.cz.*"])
        self.name = "aktualneCZ"


class IdnesCZ(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("idnes_cz_v2", [r".*idnes\.cz.*"])
        self.router.register_route("idnes_cz_v1", [r".*idnes\.cz.*"])
        self.name = "idnesCZ"


class DenikCZ(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("denik_cz_v1", [r".*denik\.cz.*"])
        self.router.register_route("denik_cz_v2", [r".*denik\.cz.*"])
        self.router.register_route("denik_cz_v3", [r".*denik\.cz.*"])
        self.name = "denikCZ"


class IhnedCZ(ExtractSameTest):
    def setUp(self) -> None:
        super().setUp()
        self.router.register_route("ihned_cz_v1", [r".*ihned\.cz.*"])
        self.router.register_route("ihned_cz_v2", [r".*ihned\.cz.*"])
        self.router.register_route("ihned_cz_v3", [r".*ihned\.cz.*"])
        self.name = "ihnedCZ"
