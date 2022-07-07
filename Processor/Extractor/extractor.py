from abc import ABC, abstractmethod
from email import charset
import logging
import re
from typing import Any, Dict
from utils import PipeMetadata


class BaseExtractor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def filter(self, response: str, metadata: PipeMetadata) -> bool:
        raise NotImplementedError()

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[Any, Any] | None:
        if self.filter(response, metadata) is False:
            logging.info(f"{metadata.domain_record.url}: Filter failed")
            return None

        article = self.preprocess(response, metadata)
        result = self.extract_no_preprocess(article, metadata)
        if result is None:
            logging.info(f"{metadata.domain_record.url}: Extract failed")
        return result

    @abstractmethod
    def extract_no_preprocess(
        self, response: str, metadata: PipeMetadata
    ) -> Dict[Any, Any] | None:
        raise NotImplementedError()

    def preprocess(self, response: str, metadata: PipeMetadata) -> str:
        linux = response.replace("\r\n", "\n")
        charset = metadata.http_header.get("charset")
        if charset is None:
            html_charset_match = re.search(r"(?<=charset=)\s*\"([\w-]+)\"", linux)
            if html_charset_match is not None:
                charset = html_charset_match.group(1).lower()
        if charset is None:
            charset = metadata.domain_record.encoding

        decoded = linux.encode(metadata.domain_record.encoding, "replace").decode(
            charset, "replace"
        )
        return decoded
