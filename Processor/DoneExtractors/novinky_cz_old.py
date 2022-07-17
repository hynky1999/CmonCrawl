from datetime import datetime
from typing import Any, Dict

from bs4 import BeautifulSoup
from Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    cz_date_transform,
    format_date_transform,
    headline_transform,
    keywords_transform,
    remove_day_transform,
)
from utils import PipeMetadata


class NovinkyCZOldExtractor(ArticleExtractor):
    TO = datetime(2019, 8, 6)

    def __init__(self):
        super().__init__(
            {
                "keywords": "meta[name='keywords']",
                "brief": "meta[name='description']",
                "headline": "title",
            },
            {
                "headline": [get_text_transform, headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
                "keywords": [get_attribute_transform("content"), keywords_transform],
            },
            {
                "content": "div#articleBody",
                "author": "p.articleAuthors",
                "publication_date": "p#articleDate",
                "category": "div#navigation > ul > li:nth-child(2) > a",
                "keywords": "div#navigation > p",
                "brief": "p.perex",
            },
            {
                "content": article_content_transform(),
                "author": [get_text_transform, author_transform],
                "publication_date": [
                    get_text_transform,
                    # Town name split
                    lambda x: x.split("-")[0],
                    cz_date_transform,
                    remove_day_transform,
                    format_date_transform("%d. %m. %Y, %H:%M"),
                ],
                "category": [get_text_transform, category_transform],
                "keywords": [
                    get_tags_transform("p a[href*='novinky.cz']"),
                    get_text_list_transform(","),
                    keywords_transform,
                ],
                "brief": [get_text_transform, brief_transform],
            },
            "div#main",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict: Dict[str, Any] = {
            "comments_num": None,
        }
        return extracted_dict


extractor = NovinkyCZOldExtractor()
