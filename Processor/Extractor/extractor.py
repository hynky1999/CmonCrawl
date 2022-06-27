from abc import ABC, abstractmethod
from Processor.Downloader.download import DEFAULT_ENCODE
from Processor.utils import PipeMetadata
import chardet

class BaseExtractor(ABC):
    def __init__(self):
        pass

    def filter(self, resopnse: str, metadata: PipeMetadata):
        # Leaves everything in
        return True

    def extract(self, response: str, metadata: PipeMetadata) -> str | None:
        if self.filter(response, metadata) is False:
            return None

        article = self.preprocess(response, metadata)
        return self.extract_no_preprocess(article, metadata)

    @abstractmethod
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata) -> str:
        raise NotImplementedError()

    def preprocess(self, response: str, metadata: PipeMetadata) -> str:
        encoding = self.__guess_encoding(response, metadata)
        decoded = response.encode(DEFAULT_ENCODE).decode(encoding)
        linux_decoded = decoded.replace("\r\n", "\n")
        return linux_decoded

    def __guess_encoding(self, response: str, metadata: PipeMetadata) -> str:
        if metadata.domain_record.encoding is not None:
            return metadata.domain_record.encoding

        encoding = chardet.detect(response.encode(DEFAULT_ENCODE))['encoding']
        if encoding is None:
            encoding = DEFAULT_ENCODE
        return encoding


