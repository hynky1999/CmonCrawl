from bs4 import BeautifulSoup
from Processor.App.Extractor.extractor import BaseExtractor
from Processor.App.processor_utils import PipeMetadata


NAME = "AAA"


class Extractor(BaseExtractor):
    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        return None

    def filter(self, response: str, metadata: PipeMetadata) -> bool:
        return True


extractor = Extractor()
