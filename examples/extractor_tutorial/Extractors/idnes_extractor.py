from typing import Any, Dict

from bs4 import BeautifulSoup

from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.processor.pipeline.extractor import BaseExtractor


class IdnesExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()

    def extract_soup(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any] | None:
        maybe_body = soup.select_one("body")
        if maybe_body is None:
            return None

        return {
            "body": maybe_body.get_text(),
        }


extractor = IdnesExtractor()
