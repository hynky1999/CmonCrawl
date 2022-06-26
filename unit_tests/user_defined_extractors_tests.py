from datetime import datetime
import json
from pathlib import Path
import re
import unittest
from Aggregator.index_query import DomainRecord
from Processor.Extractor.extractor import BaseExtractor
from Processor.utils import PipeMetadata
from UserDefinedExtractors.aktualne_cz import Extractor as AktualneExtractor
from article_utils.article_data import ArticleData

SITES_PATH = Path("unit_tests") / "sites"
TRUTH_JSONS = "truth"
TEST_ARTICLES = "test_articles"


def data_from_truth_and_site(article: Path, extractor: BaseExtractor):
    with open(article, "r") as f:
        content = f.read()

    url = re.search('og:url" content="(.*)"', content).group(1)
    metadata = PipeMetadata(
        DomainRecord(
            filename=article.name,
            url=url,
            offset=0,
            length=100,
        )
    )

    extractor.extract(content, metadata)
    article_data = metadata.article_data

    truth_path = article.parent.parent / TRUTH_JSONS / (article.stem + ".json")
    with open(truth_path, "r") as f:
        expected = json.load(f)
    expected["publication_date"] = datetime.fromisoformat(
        expected.get("publication_date")
    )
    return article_data, ArticleData(**expected)


class AktualneCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.aktualne_extractor = AktualneExtractor()

    def test_all_articles(self):
        print(Path.cwd())
        for article in (SITES_PATH / "aktualneCZ" / TEST_ARTICLES).iterdir():
            site, truth = data_from_truth_and_site(article, self.aktualne_extractor)

            # Test all attributes
            for key in truth.__dict__:
                self.assertEqual(getattr(site, key), getattr(truth, key))
