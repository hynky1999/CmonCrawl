from abc import abstractmethod
import logging
from typing import Any, Dict

from bs4 import BeautifulSoup
from Extractor.extractor import BaseExtractor

from utils import PipeMetadata
from ArticleUtils.article_utils import REQUIRED_FIELDS


class ArticleExtractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")
        extracted_dict = self.article_extract(soup, metadata)
        if not self.check_required(extracted_dict, metadata):
            return None

        # Name
        # Assumes headline is required
        assert "headline" in REQUIRED_FIELDS

        metadata.name = metadata.domain_record.url.replace("/", "_")[:30]

        metadata.article_data = extracted_dict
        return extracted_dict

    def check_required(self, extracted_dict: Dict[Any, Any], metadata: PipeMetadata):
        for key, value in REQUIRED_FIELDS.items():
            if key not in extracted_dict:
                logging.warn(
                    f"{metadata.domain_record.url} Extractor {self.__class__.__name__}: failed to extract {key}"
                )
                return False

            if value and extracted_dict[key] is None:
                logging.warn(
                    f"{metadata.domain_record.url} Extractor {self.__class__.__name__}: None for key: {key} is not allowed"
                )
                return False
        return True

    @abstractmethod
    def article_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[Any, Any]:
        raise NotImplementedError()

        # Check if it contains required
