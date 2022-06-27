from Processor.Extractor.extractor import BaseExtractor
from Processor.utils import PipeMetadata
from bs4 import BeautifulSoup


class Extractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")
        head = soup.find("head")
        if head is not None:
            charset = head.find("charset")
            del charset

        return str(soup)

    def filter(self, response: str, metadata: PipeMetadata):
        return True
