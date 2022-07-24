from datetime import datetime
from typing import Any, Dict

from bs4 import BeautifulSoup
from ArticleUtils.article_extractor import ArticleExtractor
from ArticleUtils.article_utils import (
    ALLOWED_H,
    TABLE_TAGS,
    LIST_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    comments_num_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from Extractor.extractor_utils import (
    get_attribute_transform,
    get_tag_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from processor_utils import PipeMetadata


class DenikV1Extractor(ArticleExtractor):
    TO = datetime(2012, 2, 29)

    def __init__(self):
        super().__init__(
            {
                "headline": "title",
                "keywords": "meta[name='keywords']",
            },
            {
                "keywords": [
                    get_attribute_transform("content"),
                    lambda x: x.split(" "),
                    lambda x: ",".join(x),
                    keywords_transform,
                ],
                "headline": [get_text_transform, headline_transform],
            },
            {
                "content": ".pane > .bbtext",
                "brief": ".perex p.info",
                "author": ".det-top > .a-img",
                "comments_num": ".sum > a.comm",
                "category": ".cookie-nav > p > span:nth-child(3)",
            },
            {
                "content": article_content_transform(),
                "brief": [get_text_transform, brief_transform],
                "author": [
                    get_text_transform,
                    author_transform,
                ],
                "comments_num": [get_text_transform, comments_num_transform],
                "category": [get_text_transform, category_transform],
            },
            "#content",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict = {
            "publication_date": date_complex_extract(
                soup,
                "#content .sum > span.date",
                "%d.%m.%Y %H:%M",
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
            ),
            "author": self.custom_extract_author(soup),
        }
        return extracted_dict

    def custom_extract_author(self, soup: BeautifulSoup):
        author_tag = get_tag_transform(
            ".page-content .bbtext > p[style='text-align: right;']"
        )(soup)
        if author_tag:
            return author_transform(get_text_transform(author_tag))

        return None


extractor = DenikV1Extractor()
