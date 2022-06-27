from Processor.Extractor.extractor import BaseExtractor
from Processor.utils import PipeMetadata


NAME = "BBB"


class Extractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata):
        return None

    def filter(self, response: str, metadata: PipeMetadata) -> bool:
        return True
