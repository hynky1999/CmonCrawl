from bs4 import BeautifulSoup
from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.processor.pipeline.extractor import BaseExtractor
from cmoncrawl.processor.extraction.utils import chain_transforms

NAME = "test_extractor"


class MyExtractor(BaseExtractor):
    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        # get  title
        title = chain_transforms([lambda x: x.title])(soup.title)
        return {"title": soup.title}


extractor = MyExtractor()
