from typing import Any, Callable, Dict, List

from bs4 import BeautifulSoup
from Processor.App.ArticleUtils.article_utils import (
    must_exist_filter,
    must_not_exist_filter,
)
from Processor.App.Extractor.extractor import BaseExtractor
from Processor.App.Extractor.extractor_utils import combine_dicts, extract_transform

from Processor.App.processor_utils import PipeMetadata, metadata_logger

# All keys are required if value is True then the extracted value must be not None
# and not empty


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
        header_css_dict: Dict[str, str] = {},
        header_extract_dict: Dict[
            str, List[Callable[[Any], Any]] | Callable[[Any], Any]
        ] = {},
        article_css_dict: Dict[str, str] = {},
        article_extract_dict: Dict[
            str, List[Callable[[Any], Any]] | Callable[[Any], Any]
        ] = {},
        article_css_selector: str = "body",
        filter_must_exist: List[str] = [],
        filter_must_not_exist: List[str] = [],
        filter_allowed_domain_prefixes: List[str] | None = None,
        required_fields: Dict[str, bool] = REQUIRED_FIELDS,
        non_empty: bool = True,
    ):
        self.header_css_dict = header_css_dict
        self.header_extract_dict = header_extract_dict
        self.article_css_dict = article_css_dict
        self.article_extract_dict = article_extract_dict
        self.article_css_selector = article_css_selector
        self.filter_must_exist = filter_must_exist
        self.filter_must_not_exist = filter_must_not_exist
        self.filter_allowed_domain_prefixes = filter_allowed_domain_prefixes
        self.required_fields = required_fields
        self.non_empty: bool = non_empty

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[Any, Any] | None:
        return super().extract(response, metadata)

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict = self.article_extract(soup, metadata)
        if not self.check_required(extracted_dict, metadata):
            return None

        metadata.name = metadata.domain_record.url.replace("/", "_")[:80]
        extracted_dict["url"] = metadata.domain_record.url
        return extracted_dict

    def custom_filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        return True

    def custom_filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        return True

    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        if metadata.http_header.get("http_response_code", 200) != 200:
            metadata_logger.warn(
                f"Invalid Status: {metadata.http_header.get('http_response_code', 0)}",
                extra={"domain_record": metadata.domain_record},
            )
            return False

        if self.custom_filter_raw(response, metadata) is False:
            return False
        return True

    def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        if not must_exist_filter(soup, self.filter_must_exist):
            return False

        if not must_not_exist_filter(soup, self.filter_must_not_exist):
            return False

        if (
            self.filter_allowed_domain_prefixes is not None
            and metadata.url_parsed is not None
            and metadata.url_parsed.netloc.split(".")[0]
            not in self.filter_allowed_domain_prefixes
        ):
            return False

        if self.custom_filter_soup(soup, metadata) is False:
            return False

        return True

    def check_required(self, extracted_dict: Dict[Any, Any], metadata: PipeMetadata):
        for key, value in self.required_fields.items():
            if key not in extracted_dict:
                metadata_logger.warn(
                    f"{self.__class__.__name__}: failed to extract {key}",
                    extra={"domain_record": metadata.domain_record},
                )
                return False
            extracted_val = extracted_dict[key]
            if value:
                if extracted_val is None:
                    metadata_logger.warn(
                        f"{self.__class__.__name__}: None for key: {key} is not allowed",
                        extra={"domain_record": metadata.domain_record},
                    )
                    return False

                if self.non_empty:
                    if isinstance(extracted_val, str) and extracted_val == "":
                        metadata_logger.warn(
                            f"{self.__class__.__name__}: empty string for key: {key} is not allowed",
                            extra={"domain_record": metadata.domain_record},
                        )
                        return False
                    if isinstance(extracted_val, list) and len(extracted_val) == 0:
                        metadata_logger.warn(
                            f"{self.__class__.__name__}: empty list for key: {key} is not allowed",
                            extra={"domain_record": metadata.domain_record},
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
        extracted_dict = combine_dicts(
            [extracted_head, extracted_article, custom_extract]
        )
        return extracted_dict

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        # Allows for custom extraction of values
        return {}
