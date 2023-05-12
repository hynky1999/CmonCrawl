from abc import ABC, abstractmethod
from typing import Any, Dict

from bs4 import BeautifulSoup

from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.common.loggers import metadata_logger


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

    """

    def __init__(self, encoding: str | None = None):
        self.encoding = encoding

    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        # If raw fails bs4 will not be used -> speed
        return True

    def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        # slow but has more info
        return True

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[str, Any] | None:
        if self.filter_raw(response, metadata) is False:
            metadata_logger.warn(
                "Droped due to raw filter",
                extra={"domain_record": metadata.domain_record},
            )
            return None

        article = self.preprocess(response, metadata)
        soup = BeautifulSoup(article, "html.parser")
        if self.filter_soup(soup, metadata) is False:
            metadata_logger.warn(
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

    def preprocess(self, response: str, metadata: PipeMetadata) -> str:
        linux = response.replace("\r\n", "\n")
        # Sorted set pythonic way
        encodings: Dict[str, int] = {}
        if self.encoding is not None:
            encodings[self.encoding] = 1
        if metadata.domain_record.encoding is not None:
            encodings[metadata.domain_record.encoding] = 1
        http_split = metadata.http_header.get("Content-Type", "").split("charset=")
        if len(http_split) > 1 and http_split[1] != "":
            encodings[http_split[-1]] = 1

        # Fallbacks
        encodings["utf-8"] = 1

        encoded = linux.encode(metadata.encoding)
        for encoding in encodings:
            try:
                decoded = encoded.decode(encoding)
                metadata.encoding = encoding
                break
            except ValueError:
                metadata_logger.warn(
                    f"Failed to decode with {encoding}",
                    extra={"domain_record": metadata.domain_record},
                )
        else:
            raise ValueError("Failed to decode")

        return decoded


class HTMLExtractor(BaseExtractor):
    """
    Dummy Extractor which simply extracts the html

    Args:
        filter_non_ok (bool, optional): If True, only 200 status codes will be extracted. Defaults to True.
    """

    def __init__(self, filter_non_ok: bool = True):
        super().__init__()
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
            metadata_logger.warning(
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
        super().__init__()
        self.filter_non_ok = filter_non_ok

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        metadata.name = (
            metadata.domain_record.url.replace("/", "_")[:100]
            if metadata.domain_record.url is not None
            else "unknown"
        )
        result_dict: Dict[str, Any] = {
            "domain_record": metadata.domain_record.to_dict()
        }

        return result_dict

    def filter_raw(self, response: str, metadata: PipeMetadata):
        if (
            self.filter_non_ok
            and metadata.http_header.get("http_response_code", 200) != 200
        ):
            metadata_logger.warning(
                f"Status: {metadata.http_header.get('http_response_code', 0)}",
                extra={"domain_record": metadata.domain_record},
            )
            return False
        return True
