from bs4 import BeautifulSoup
from App.Extractor.extractor import BaseExtractor
from App.processor_utils import PipeMetadata


NAME = "BBB"


class Extractor(BaseExtractor):
    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        return None

    def filter(self, response: str, metadata: PipeMetadata) -> bool:
        return True


extractor = Extractor()
