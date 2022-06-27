from abc import ABC, abstractmethod
from typing import Any, Dict
from Processor.utils import PipeMetadata


class BaseExtractor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def filter(self, response: str, metadata: PipeMetadata) -> bool:
        raise NotImplementedError()

    def extract(self, response: str, metadata: PipeMetadata) -> Dict[Any, Any] | None:
        if self.filter(response, metadata) is False:
            return None

        article = self.preprocess(response, metadata)
        return self.extract_no_preprocess(article, metadata)

    @abstractmethod
    def extract_no_preprocess(
        self, response: str, metadata: PipeMetadata
    ) -> Dict[Any, Any] | None:
        raise NotImplementedError()

    def preprocess(self, response: str, metadata: PipeMetadata) -> str:
        linux_decoded = response.replace("\r\n", "\n")
        return linux_decoded
