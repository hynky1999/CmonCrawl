from datetime import datetime
from importlib.metadata import metadata
import json
from pathlib import Path
import unittest
from Router.router import Router


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


class IrozhlasTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("irozhlas_cz", [r".*irozhlas\.cz.*"])
        self.name = "rozhlasCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)


class SeznamZpravyCZ(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("irozhlas_cz", [r".*irozhlas\.cz.*"])
        self.router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
        self.name = "seznamzpravyCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)


class NovinkyCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("novinky_cz_v2", [r".*novinky\.cz.*"])
        self.router.register_route("novinky_cz_v1", [r".*novinky\.cz.*"])
        self.name = "novinkyCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                "novinky.cz", metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)


class AktualneCZTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("aktualne_cz_v3", [r".*aktualne\.cz.*"])
        self.router.register_route("aktualne_cz_v2", [r".*aktualne\.cz.*"])
        self.router.register_route("aktualne_cz_v1", [r".*aktualne\.cz.*"])
        self.name = "aktualneCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                "aktualne.cz", metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)


class IdnesCZ(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("idnes_cz_v2", [r".*idnes\.cz.*"])
        self.router.register_route("idnes_cz_v1", [r".*idnes\.cz.*"])
        self.name = "idnesCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)


class DenikCZ(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("denik_cz_v1", [r".*denik\.cz.*"])
        self.router.register_route("denik_cz_v2", [r".*denik\.cz.*"])
        self.router.register_route("denik_cz_v3", [r".*denik\.cz.*"])
        self.name = "denikCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)


class IhnedCZ(unittest.TestCase):
    def setUp(self) -> None:
        self.router = Router()
        self.router.load_modules(str(Path("DoneExtractors").absolute()))
        self.router.register_route("ihned_cz_v1", [r".*ihned\.cz.*"])
        self.router.register_route("ihned_cz_v2", [r".*ihned\.cz.*"])
        self.router.register_route("ihned_cz_v3", [r".*ihned\.cz.*"])
        self.name = "ihnedCZ"

    def test_extract_articles(self):
        for article_path in (SITES_PATH / self.name / TEST_ARTICLES).iterdir():
            article_test, metadata_test = parse_article(article_path)
            extractor = self.router.route(
                metadata_test.domain_record.url,
                metadata_test.domain_record.timestamp,
                metadata_test,
            )
            extracted_test = extractor.extract(article_test, metadata_test)
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
            extractor = self.router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            extracted_test = extractor.extract(article, metadata)
            self.assertIsNone(extracted_test)
