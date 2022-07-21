from bs4 import BeautifulSoup
from Extractor.extractor import BaseExtractor
from utils import PipeMetadata, metadata_logger


class Extractor(BaseExtractor):
    ENCODING = "utf-8"

    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        metadata.name = metadata.domain_record.url.replace("/", "_")[:100]
        return {"html": str(soup)}

    def filter_raw(self, response: str, metadata: PipeMetadata):
        if metadata.http_header.get("http_response_code", 200) != 200:
            metadata_logger.warning(
                f"Status: {metadata.http_header.get('http_response_code', 0)}",
                extra={"domain_record": metadata.domain_record},
            )
            return False
        return True


extractor = Extractor()
