from Processor.Extractor.extractor import BaseExtractor
from Processor.utils import PipeMetadata


class Extractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata) -> str:
        return response