from bs4 import BeautifulSoup
from cmoncrawl.processor.pipeline.extractor import BaseExtractor
from cmoncrawl.common.types import PipeMetadata

NAME = "BBB"


class Extractor(BaseExtractor):
    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        return None

    def filter(self, response: str, metadata: PipeMetadata) -> bool:
        return True


extractor = Extractor()
