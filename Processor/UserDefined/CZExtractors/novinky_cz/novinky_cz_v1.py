from datetime import datetime
from typing import Any, Dict

from bs4 import BeautifulSoup
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor

from Processor.App.ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from Processor.App.processor_utils import PipeMetadata


class NovinkyCZV1Extractor(ArticleExtractor):
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
                "category": "div#navigation > ul > li:nth-child(2) > a",
                "keywords": "div#navigation > p",
                "brief": "p.perex",
            },
            {
                "content": article_content_transform(),
                "author": [get_text_transform, author_transform],
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
            "publication_date": date_complex_extract(
                soup,
                "p#articleDate",
                "%d. %m. %Y, %H:%M",
                fallback=metadata.domain_record.timestamp,
                remove_additional_info=True,
                remove_day=True,
                cz_month=True,
            ),
        }
        return extracted_dict


extractor = NovinkyCZV1Extractor()
