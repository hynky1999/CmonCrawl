from datetime import datetime
import re
from typing import Any, Dict

from bs4 import BeautifulSoup

from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
from Processor.App.ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    comments_num_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tag_transform,
    get_text_transform,
)
from Processor.App.processor_utils import PipeMetadata


date_sub = re.compile(r"\(aktualizováno.*")


class DenikV1Extractor(ArticleExtractor):
    TO = datetime(2011, 5, 5)
    ENCODING = "windows-1250"

    def __init__(self):
        super().__init__(
            {
                "headline": "title",
            },
            {
                "headline": [get_text_transform, headline_transform],
            },
            {
                "content": ".d-text",
                "category": ".site-name > a",
                "author": ".d-autor > form > span",
                "comments_num": ".d-source > a.disc",
                "keywords": "#tagy > .d-elem-in",
            },
            {
                "content": article_content_transform(),
                "category": [get_text_transform, category_transform],
                "author": [get_text_transform, author_transform],
                "comments_num": [get_text_transform, comments_num_transform],
                "keywords": [get_text_transform, keywords_transform],
            },
            "#main",
        )

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        extracted_dict = {
            "publication_date": date_complex_extract(
                soup,
                ".d-source > span",
                ["%d. %m. %Y %H:%M", "%d. %m. %Y"],
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
                custom_cleaner=lambda x: date_sub.sub("", x),
            ),
            "brief": self.custom_brief_extract(soup),
        }
        return extracted_dict

    def custom_brief_extract(self, soup: BeautifulSoup):
        brief_tag = get_tag_transform(".d-perex")(soup)
        if brief_tag is not None:
            brief = brief_transform(get_text_transform(brief_tag))
            if brief != "":
                return brief

        brief_tag = get_tag_transform("head > meta[name='description']")(soup)
        if brief_tag is not None:
            brief = brief_transform(get_attribute_transform("content")(brief_tag))
            if brief != "":
                return brief_transform(brief)
        return None


extractor = DenikV1Extractor()
