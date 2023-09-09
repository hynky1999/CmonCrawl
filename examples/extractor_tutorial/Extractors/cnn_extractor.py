from typing import Any, Dict
from bs4 import BeautifulSoup
from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.processor.pipeline.extractor import BaseExtractor


class CNNExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()

    def extract_soup(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any] | None:
        maybe_title = soup.select_one("div.headline > h1")
        maybe_content = soup.select_one("#posts")
        if maybe_title is None or maybe_content is None:
            return None

        title = maybe_title.text
        content = maybe_content.text
        return {
            "title": title,
            "content": content,
        }


extractor = CNNExtractor()
