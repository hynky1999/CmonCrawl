from datetime import datetime
import json
from pathlib import Path
import unittest
from UserDefinedExtractors.aktualne_cz import Extractor as AktualneExtractor


from process_article import parse_article

SITES_PATH = Path("unit_tests") / "sites"
TRUTH_JSONS = "truth"
TEST_ARTICLES = "test_articles"
FILTER_ARTICLES = "filter_articles"


def parse_extracted_json(extracted_path: Path):
    with open(extracted_path, "r") as f:
        extracted = json.load(f)
    extracted["publication_date"] = datetime.fromisoformat(
        extracted["publication_date"]
    )
    return extracted


class AktualneCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.aktualne_extractor = AktualneExtractor()

    def test_extract_articles(self):
        for article_path in (SITES_PATH / "aktualneCZ" / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extracted_test = self.aktualne_extractor.extract(
                article_test, metadata_test
            )
            extracted_truth = parse_extracted_json(
                article_path.parent.parent / TRUTH_JSONS / (article_path.stem + ".json")
            )
            self.assertIsNotNone(extracted_test)
            # Test all attributes
            for key in extracted_truth:
                self.assertEqual(extracted_test.get(key), extracted_truth.get(key))

    def test_filter_articles(self):
        # 1 Tests wrong section -> Aktualne+
        # 2 Tests wrong author(Aktualne) -> missing
        # 3 Tests blog section
        for article_f in (SITES_PATH / "aktualneCZ" / FILTER_ARTICLES).iterdir():
            article, metadata = parse_article(article_f)
            self.assertIsNone(self.aktualne_extractor.extract(article, metadata))
