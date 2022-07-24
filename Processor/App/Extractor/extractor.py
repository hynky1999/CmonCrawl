from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict

from bs4 import BeautifulSoup
from processor_utils import PipeMetadata, metadata_logger


class BaseExtractor(ABC):
    ENCODING: str | None = None
    SINCE: datetime | None = None
    TO: datetime | None = None

    def __init__(self):
        pass

    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        return True

    def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        return True

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[Any, Any] | None:
        if self.filter_raw(response, metadata) is False:
            metadata_logger.warn(
                "Failed to filter raw", extra={"domain_record": metadata.domain_record}
            )
            return None

        article = self.preprocess(response, metadata)
        soup = BeautifulSoup(article, "html.parser")
        if self.filter_soup(soup, metadata) is False:
            metadata_logger.warn(
                "Failed to filter soup", extra={"domain_record": metadata.domain_record}
            )
            return None

        return self.extract_soup(soup, metadata)

    @abstractmethod
    def extract_soup(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[Any, Any] | None:
        raise NotImplementedError()

    def preprocess(self, response: str, metadata: PipeMetadata) -> str:
        linux = response.replace("\r\n", "\n")
        # Sorted set
        encodings: Dict[str, int] = {}
        if self.ENCODING is not None:
            encodings[self.ENCODING] = 1
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
