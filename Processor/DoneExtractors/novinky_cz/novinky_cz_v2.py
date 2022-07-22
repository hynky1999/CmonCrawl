from datetime import datetime
from typing import Any, Dict

from ArticleUtils.article_extractor import ArticleExtractor
from ArticleUtils.article_utils import (
    ALLOWED_H,
    LIST_TAGS,
    TABLE_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from bs4 import BeautifulSoup, Tag
from Extractor.extractor_utils import (
    get_attribute_transform,
    get_tag_transform,
    get_text_transform,
)
from utils import PipeMetadata

allowed_classes_div = {
    # text
    "g_c6",
    "g_c9",
    "f_cZ",
    "f_eB",
    # images
    "g_cJ",
}


def article_transform_fc(tag: Tag):
    if tag.name in ["p", "span", "figcaption", *ALLOWED_H, *TABLE_TAGS, *LIST_TAGS]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name == "div" and len(allowed_classes_div.intersection(classes)) > 0:
        return True

    return False


class NovinkyCZV2Extractor(ArticleExtractor):
    SINCE = datetime(2019, 8, 6)

    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "keywords": "meta[name='keywords']",
                "brief": "meta[property='og:description']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
                "keywords": [get_attribute_transform("content"), keywords_transform],
            },
            {
                "content": 'article > div[data-dot-data=\'{"component":"article-content"}\'] > div',
                # Very improvvised for compatibility
                "category": "[data-dot*='navi'] > a[href*=novinky]:nth-child(2)",
            },
            {
                "content": article_content_transform(
                    fc_eval=article_transform_fc,
                ),
                "author": [get_text_transform, author_transform],
                "category": [get_text_transform, category_transform],
            },
            "main[role='main']",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict: Dict[str, Any] = {
            "comments_num": None,
            "publication_date": date_complex_extract(
                soup,
                ".ogm-article-author-social .atm-date-formatted",
                "%d. %m. %Y, %H:%M",
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
            ),
            "author": self.custom_extract_author(soup, metadata),
        }
        return extracted_dict

    def custom_extract_author(self, soup: BeautifulSoup, metadata: PipeMetadata):
        author = None
        tag = get_tag_transform("div.ogm-article-author__texts > div:nth-child(2)")(
            soup
        )
        if tag is not None:
            author = get_text_transform(tag)
            if author is not None:
                return author_transform(author)

        tag = get_tag_transform('div[data-dot-data=\'{"click":"author"}\']')(soup)
        if tag is not None:
            author = get_text_transform(tag)
            if author is not None:
                return author_transform(author)

        return author


extractor = NovinkyCZV2Extractor()
