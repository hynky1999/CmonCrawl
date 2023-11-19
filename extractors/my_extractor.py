from bs4 import BeautifulSoup

from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.processor.pipeline.extractor import BaseExtractor


class MyExtractor(BaseExtractor):
    def __init__(self):
        # you can force a specific encoding if you know it
        super().__init__(encoding=None)

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        # here you can extract the data you want from the soup
        # and return a dict with the data you want to save
        body = soup.select_one("body")
        if body is None:
            return None
        return {"body": body.get_text()}

    # You can also override the following methods to drop the files you don't want to extracti
    # Return True to keep the file, False to drop it
    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        return True

    def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
        return True


# Make sure to instantiate your extractor into extractor variable
# The name must match so that the framework can find it
extractor = MyExtractor()
