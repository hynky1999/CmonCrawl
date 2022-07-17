from datetime import datetime
import json
from pathlib import Path
import unittest
from UserDefinedExtractors.aktualne_cz import Extractor as AktualneExtractor
from UserDefinedExtractors.idnes_cz import Extractor as IdnesExtractor
from DoneExtractors.seznamzpravy_cz import Extractor as SeznamzpravyExtractor
from DoneExtractors.irozhlas_cz import Extractor as IrozhlasExtractor
from DoneExtractors.novinky_cz import Extractor as NovinkyExtractor


from process_article import parse_article

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


class AktualneCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = AktualneExtractor()
        self.name = "aktualneCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extracted_test = self.extractor.extract(article_test, metadata_test)
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
        for article_f in (SITES_PATH / self.name / FILTER_ARTICLES).iterdir():
            article, metadata = parse_article(article_f)
            self.assertIsNone(self.extractor.extract(article, metadata))


class IdnesCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = IdnesExtractor()
        self.name = "idnesCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extracted_test = self.extractor.extract(article_test, metadata_test)
            extracted_truth = parse_extracted_json(
                article_path.parent.parent / TRUTH_JSONS / (article_path.stem + ".json")
            )
            self.assertIsNotNone(extracted_test)
            # Test all attributes
            for key in extracted_truth:
                self.assertEqual(extracted_test.get(key), extracted_truth.get(key))

    def test_filter_articles(self):
        for article_f in (SITES_PATH / self.name / FILTER_ARTICLES).iterdir():
            article, metadata = parse_article(article_f)
            self.assertIsNone(self.extractor.extract(article, metadata))


class SeznamzpravyCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = SeznamzpravyExtractor()
        self.name = "seznamzpravyCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extracted_test = self.extractor.extract(article_test, metadata_test)
            extracted_truth = parse_extracted_json(
                article_path.parent.parent / TRUTH_JSONS / (article_path.stem + ".json")
            )
            self.assertIsNotNone(extracted_test)
            # Test all attributes
            for key in extracted_truth:
                self.assertEqual(extracted_test.get(key), extracted_truth.get(key))

    def test_filter_articles(self):
        for article_f in (SITES_PATH / self.name / FILTER_ARTICLES).iterdir():
            article, metadata = parse_article(article_f)
            self.assertIsNone(self.extractor.extract(article, metadata))


class IrozhlasTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = IrozhlasExtractor()
        self.name = "rozhlasCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extracted_test = self.extractor.extract(article_test, metadata_test)
            extracted_truth = parse_extracted_json(
                article_path.parent.parent / TRUTH_JSONS / (article_path.stem + ".json")
            )
            print(metadata_test.domain_record.url, metadata_test.domain_record.filename)
            self.assertIsNotNone(extracted_test)
            # Test all attributes
            for key in extracted_truth:
                self.assertEqual(extracted_test.get(key), extracted_truth.get(key))

    def test_filter_articles(self):
        for article_f in (SITES_PATH / self.name / FILTER_ARTICLES).iterdir():
            article, metadata = parse_article(article_f)
            self.assertIsNone(self.extractor.extract(article, metadata))


class NovinkyCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = NovinkyExtractor()
        self.name = "novinkyCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extracted_test = self.extractor.extract(article_test, metadata_test)
            extracted_truth = parse_extracted_json(
                article_path.parent.parent / TRUTH_JSONS / (article_path.stem + ".json")
            )
            print(metadata_test.domain_record.url, metadata_test.domain_record.filename)
            self.assertIsNotNone(extracted_test)
            # Test all attributes
            for key in extracted_truth:
                self.assertEqual(extracted_test.get(key), extracted_truth.get(key))

    def test_filter_articles(self):
        for article_f in (SITES_PATH / self.name / FILTER_ARTICLES).iterdir():
            article, metadata = parse_article(article_f)
            self.assertIsNone(self.extractor.extract(article, metadata))
