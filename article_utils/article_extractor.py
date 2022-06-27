from abc import abstractmethod
from typing import Any, Dict

from bs4 import BeautifulSoup
from Processor.Extractor.extractor import BaseExtractor

from Processor.utils import PipeMetadata
from article_utils.article_utils import REQUIRED_FIELDS


class ArticleExtractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")
        extracted_dict = self.article_extract(soup, metadata)
        if not self.check_required(extracted_dict):
            return None

        # Name
        # Assumes headline is required
        assert "headline" in REQUIRED_FIELDS

        metadata.name = extracted_dict.get("headline")
        if metadata.name is not None:
            metadata.name = f"{metadata.url_parsed.netloc.split('.')[-1]}_{metadata.name.strip()[:12]}"

        return extracted_dict

    def check_required(self, extracted_dict: Dict[Any, Any]):
        for key, value in REQUIRED_FIELDS.items():
            if key not in extracted_dict:
                return False

            if value and extracted_dict[key] is None:
                return False

        return True

    @abstractmethod
    def article_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[Any, Any]:
        raise NotImplementedError()

        # Check if it contains required
