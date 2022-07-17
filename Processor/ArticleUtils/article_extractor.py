from abc import abstractmethod
import logging
from typing import Any, Callable, Dict, List

from bs4 import BeautifulSoup
from ArticleUtils.article_utils import must_exist_filter, must_not_exist_filter
from Extractor.extractor import BaseExtractor
from Extractor.extractor_utils import combine_dicts, extract_transform

from utils import PipeMetadata

# All keys are required if value if true then the extracted value must be non None
REQUIRED_FIELDS = {
    "content": True,
    "headline": True,
    "author": True,
    "brief": False,
    "publication_date": False,
    "keywords": False,
    "category": False,
    "comments_num": False,
}


class ArticleExtractor(BaseExtractor):
    def __init__(
        self,
        header_css_dict: Dict[str, str],
        header_extract_dict: Dict[
            str, List[Callable[[Any], Any]] | Callable[[Any], Any]
        ],
        article_css_dict: Dict[str, str],
        article_extract_dict: Dict[
            str, List[Callable[[Any], Any]] | Callable[[Any], Any]
        ],
        article_css_selector: str,
        filter_must_exist: Dict[str, str] = {},
        filter_must_not_exist: Dict[str, str] = {},
    ):
        self.header_css_dict = header_css_dict
        self.header_extract_dict = header_extract_dict
        self.article_css_dict = article_css_dict
        self.article_extract_dict = article_extract_dict
        self.article_css_selector = article_css_selector
        self.filter_must_exist = filter_must_exist
        self.filter_must_not_exist = filter_must_not_exist

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[Any, Any] | None:
        return super().extract(response, metadata)

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict = self.article_extract(soup, metadata)
        if not self.check_required(extracted_dict, metadata):
            return None

        metadata.name = metadata.domain_record.url.replace("/", "_")
        return extracted_dict

    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        if metadata.http_header.get("http_response_code", 200) != 200:
            logging.warn(f"Status: {metadata.http_header.get('http_response_code', 0)}")
            return False
        return True

    def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        if not must_exist_filter(soup, self.filter_must_exist):
            return False

        if not must_not_exist_filter(soup, self.filter_must_not_exist):
            return False

        return True

    def check_required(self, extracted_dict: Dict[Any, Any], metadata: PipeMetadata):
        for key, value in REQUIRED_FIELDS.items():
            if key not in extracted_dict:
                logging.warn(
                    f"{self.__class__.__name__}: failed to extract {key}",
                    extra={"metadata": metadata},
                )
                return False

            if value and extracted_dict[key] is None:
                logging.warn(
                    f"{self.__class__.__name__}: None for key: {key} is not allowed",
                    extra={"metadata": metadata},
                )
                return False
        return True

    def article_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[Any, Any]:
        extracted_head = extract_transform(
            soup.select_one("head"), self.header_css_dict, self.header_extract_dict
        )

        extracted_article = extract_transform(
            soup.select_one(self.article_css_selector),
            self.article_css_dict,
            self.article_extract_dict,
        )

        custom_extract = self.custom_extract(soup, metadata)

        # merge dicts
        extracted_dict = combine_dicts([extracted_head, extracted_article, custom_extract])
        return extracted_dict

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        return {}
