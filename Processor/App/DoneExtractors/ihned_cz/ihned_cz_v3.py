from datetime import datetime
from typing import Any, Dict

from bs4 import BeautifulSoup

from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
from Processor.App.ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    headline_transform,
    iso_date_transform,
    keywords_transform,
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from Processor.App.processor_utils import PipeMetadata


class DenikV3Extractor(ArticleExtractor):
    SINCE = datetime(2019, 5, 5)

    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "publication_date": "meta[property='article:published_time']",
                "keywords": "meta[name='keywords']",
                "brief": "meta[name='description']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
                "publication_date": [
                    get_attribute_transform("content"),
                    iso_date_transform,
                ],
                "keywords": [get_attribute_transform("content"), keywords_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
            },
            {
                "content": ".clanek-telo",
                "author": ".authors",
                "category": ".main-menu li.active",
            },
            {
                "content": article_content_transform(),
                "author": [
                    get_tags_transform("div > .name > a"),
                    get_text_list_transform(","),
                    author_transform,
                ],
                "category": [get_text_transform, category_transform],
            },
            "body",
            filter_must_not_exist=["[class*='paywall']"],
        )

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        extracted_dict = {
            "comments_num": None,
        }
        return extracted_dict


extractor = DenikV3Extractor()
