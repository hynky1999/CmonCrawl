from abc import ABC
from typing import Any, Dict
from Processor.Downloader.download import DEFAULT_ENCODE


class BaseExtractor(ABC):
    def __init__(self):
        pass

    def extract(self, response: str, pipe_params: Dict[str, Any]) -> str:
        article = self.preprocess(response, pipe_params)
        return self.extract_no_preprocess(article, pipe_params)

    def extract_no_preprocess(
        self, response: str, pipe_params: Dict[str, Any]
    ) -> str:
        raise NotImplementedError()

    def preprocess(self, response: str, pipe_params: Dict[str, Any]) -> str:
        decoded = response.encode(DEFAULT_ENCODE).decode(pipe_params.get("encoding", DEFAULT_ENCODE))
        linux_decoded = decoded.replace("\r\n", "\n")
        return linux_decoded

