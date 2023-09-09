from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from bs4 import BeautifulSoup

from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.common.loggers import metadata_logger
from cmoncrawl.processor.extraction.filters import (
    must_exist_filter,
    must_not_exist_filter,
)
from cmoncrawl.processor.extraction.utils import (
    check_required,
    combine_dicts,
    extract_transform,
)


class IExtractor(ABC):
    """
    Base class for all extractors
    """

    @abstractmethod
    def extract(self, response: str, metadata: PipeMetadata) -> Dict[str, Any] | None:
        """
        Extracts the data from the response, if the extractor fails to extract the data it should return None
        return None

        Args:
            response (str): response from the downloader
            metadata (PipeMetadata): Metadata of the response
        """
        raise NotImplementedError()


class BaseExtractor(IExtractor, ABC):
    """
    Base class for all soup extractors

    Args:
        encoding (str, optional): Default encoding to be used. Defaults to None.
        raise_on_encoding (bool, optional): If True, the extractor will raise ValueException if it fails to decode the response. Defaults to False.
    """

    def __init__(
        self,
        encoding: str | None = None,
        raise_on_encoding: bool = False,
        parser: str = "html.parser",
    ):
        self.encoding = encoding
        self.raise_on_encoding = raise_on_encoding
        self.parser = parser

    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        # If raw fails bs4 will not be used -> speed
        return True

    def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        # slow but has more info
        return True

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[str, Any] | None:
        if self.filter_raw(response, metadata) is False:
            metadata_logger.info(
                "Droped due to raw filter",
                extra={"domain_record": metadata.domain_record},
            )
            return None

        article = self.preprocess(response, metadata)
        try:
            soup = BeautifulSoup(article, self.parser)
        except:
            metadata_logger.error(
                "Failed to parse soup", extra={"domain_record": metadata.domain_record}
            )
            return None
        if self.filter_soup(soup, metadata) is False:
            metadata_logger.info(
                "Droped due to soup filter",
                extra={"domain_record": metadata.domain_record},
            )
            return None

        return self.extract_soup(soup, metadata)

    @abstractmethod
    def extract_soup(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any] | None:
        raise NotImplementedError()

    def encode(self, response: str, metadata: PipeMetadata):
        # Sorted set pythonic way, we assume that dict is sortd
        encodings: Dict[str, int] = {}
        if self.encoding is not None:
            encodings[self.encoding] = 1
        if metadata.domain_record.encoding is not None:
            encodings[metadata.domain_record.encoding] = 1
        # GET from http header
        http_split = metadata.http_header.get("Content-Type", "").split("charset=")
        if len(http_split) > 1 and http_split[1] != "":
            encodings[http_split[-1]] = 1

        # Fallbacks
        encodings["utf-8"] = 1
        encoded = response.encode(metadata.encoding)
        decoded = None
        for encoding in encodings:
            try:
                decoded = encoded.decode(encoding)
                metadata.encoding = encoding
                break
            except (LookupError, ValueError):
                metadata_logger.warn(
                    f"Failed to decode with {encoding}",
                    extra={"domain_record": metadata.domain_record},
                )

        if decoded is None:
            if self.raise_on_encoding:
                raise ValueError("Failed to decode")
            else:
                decoded = response

        return decoded

    def preprocess(self, response: str, metadata: PipeMetadata) -> str:
        response = response.replace("\r\n", "\n")
        response = self.encode(response, metadata)
        return response


class HTMLExtractor(BaseExtractor):
    """
    Dummy Extractor which simply extracts the html

    Args:
        filter_non_ok (bool, optional): If True, only 200 status codes will be extracted. Defaults to True.
        encoding (str, optional): Default encoding to be used. Defaults to None. If set, the extractor will raise ValueException if it fails to decode the response.
    """

    def __init__(self, filter_non_ok: bool = True, encoding: str | None = None):
        super().__init__(encoding=encoding, raise_on_encoding=encoding is not None)
        self.filter_non_ok = filter_non_ok

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        metadata.name = (
            metadata.domain_record.url.replace("/", "_")[:100]
            if metadata.domain_record.url is not None
            else "unknown"
        )
        result_dict: Dict[str, Any] = {"html": str(soup)}

        return result_dict

    def filter_raw(self, response: str, metadata: PipeMetadata):
        if (
            self.filter_non_ok
            and metadata.http_header.get("http_response_code", 200) != 200
        ):
            metadata_logger.info(
                f"Status: {metadata.http_header.get('http_response_code', 0)}",
                extra={"domain_record": metadata.domain_record},
            )
            return False
        return True


class DomainRecordExtractor(BaseExtractor):
    """
    Dummy Extractor which simply extracts the domain record

    Args:
        filter_non_ok (bool, optional): If True, only 200 status codes will be extracted. Defaults to True.
    """

    def __init__(self, filter_non_ok: bool = True):
        # We only extract records, don't try to encode
        super().__init__()
        self.filter_non_ok = filter_non_ok

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        metadata.name = (
            metadata.domain_record.url.replace("/", "_")[:100]
            if metadata.domain_record.url is not None
            else "unknown"
        )
        result_dict: Dict[str, Any] = {
            "domain_record": metadata.domain_record.to_dict()  # type: ignore Wrong type
        }

        return result_dict

    def filter_raw(self, response: str, metadata: PipeMetadata):
        if (
            self.filter_non_ok
            and metadata.http_header.get("http_response_code", 200) != 200
        ):
            metadata_logger.info(
                f"Status: {metadata.http_header.get('http_response_code', 0)}",
                extra={"domain_record": metadata.domain_record},
            )
            return False
        return True


class PageExtractor(BaseExtractor):
    """
    The PageExtractor is designed to extracte specific elements from a web page,
    while adding ability to choose when to extract the data.

    Args:
        header_css_dict (Dict[str, str]): A dictionary specifying the CSS selectors for the header elements.
        header_extract_dict (Dict[str, List[Callable[[Any], Any]] | Callable[[Any], Any]]): A dictionary
            specifying the extraction functions for the header elements.
            The keys must match the keys in the header_css_dict.
            The functions are applied in the order they are specified in the list.

        content_css_selector (str): The CSS selector specifying where the content elements are located.
        content_css_dict (Dict[str, str]): A dictionary specifying the CSS selectors for the content elements.
            Selectors must be relative to the content_css_selector.

        content_extract_dict (Dict[str, List[Callable[[Any], Any]] | Callable[[Any], Any]]): A dictionary
            specifying the extraction functions for the content elements.
            The keys must match the keys in the content_css_dict.
            The functions are applied in the order they are specified in the list.

        css_selectors_must_exist (List[str]): A list of CSS selectors that must exist for the extraction to proceed.
        css_selectors_must_not_exist (List[str]): A list of CSS selectors that must not exist for the extraction to proceed.
        allowed_domain_prefixes (List[str] | None): A list of allowed domain prefixes. If None, all domain prefixes are allowed.
        is_valid_extraction (Callable[[Dict[Any, Any], PipeMetadata], bool]): A function that takes in the extracted data and the metadata and returns True if the extraction is valid, False otherwise.
        encoding (str | None): The encoding to be used. If None, the default encoding is used.

    Returns:
        Dict[Any, Any] | None: A dictionary containing the extracted data, or None if the extraction failed.
    """

    def __init__(
        self,
        header_css_dict: Dict[str, str] = {},
        header_extract_dict: Dict[
            str, List[Callable[[Any], Any]] | Callable[[Any], Any]
        ] = {},
        content_css_selector: str = "body",
        content_css_dict: Dict[str, str] = {},
        content_extract_dict: Dict[
            str, List[Callable[[Any], Any]] | Callable[[Any], Any]
        ] = {},
        css_selectors_must_exist: List[str] = [],
        css_selectors_must_not_exist: List[str] = [],
        allowed_domain_prefixes: List[str] | None = None,
        is_valid_extraction: Optional[
            Callable[[Dict[Any, Any], PipeMetadata], bool]
        ] = None,
        encoding: str | None = None,
    ):
        super().__init__(encoding=encoding)
        self.header_css_dict = header_css_dict
        self.header_extract_dict = header_extract_dict
        self.article_css_dict = content_css_dict
        self.article_extract_dict = content_extract_dict
        self.article_css_selector = content_css_selector
        self.filter_must_exist = css_selectors_must_exist
        self.filter_must_not_exist = css_selectors_must_not_exist
        self.filter_allowed_domain_prefixes = allowed_domain_prefixes
        self.is_valid_extraction = is_valid_extraction

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[Any, Any] | None:
        return super().extract(response, metadata)

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict = self.article_extract(soup, metadata)
        if self.is_valid_extraction and not self.is_valid_extraction(
            extracted_dict, metadata
        ):
            return None

        metadata.name = metadata.domain_record.url.replace("/", "_")[:80]
        extracted_dict["url"] = metadata.domain_record.url
        extracted_dict["domain_record"] = metadata.domain_record.to_dict()
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
            and metadata.url_parsed.netloc.split(".")[0]
            not in self.filter_allowed_domain_prefixes
        ):
            return False

        if self.custom_filter_soup(soup, metadata) is False:
            return False

        return True

    def article_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[Any, Any]:
        extracted_head = extract_transform(
            soup.select_one("head"), self.header_css_dict, self.header_extract_dict
        )

        extracted_page = extract_transform(
            soup.select_one(self.article_css_selector),
            self.article_css_dict,
            self.article_extract_dict,
        )

        custom_extract = self.custom_extract(soup, metadata)

        # merge dicts
        extracted_dict = combine_dicts([extracted_head, extracted_page, custom_extract])
        return extracted_dict

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        # Allows for custom extraction of values
        return {}
