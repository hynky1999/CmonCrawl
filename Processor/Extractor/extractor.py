from abc import ABC, abstractmethod
import logging
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
        linux_decoded = response.replace("\r\n", "\n")
        return linux_decoded
