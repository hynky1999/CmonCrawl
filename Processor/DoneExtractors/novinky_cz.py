from datetime import datetime
import re
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
    format_date_transform,
    headline_transform,
    keywords_transform,
)
from bs4 import BeautifulSoup, Tag
from Extractor.extractor_utils import (
    extract_transform,
    get_attribute_transform,
    get_text_transform,
)
from utils import PipeMetadata

allowed_classes_div = {
    # text
    "g_c6",
    "g_c9",
    # images
    "g_cJ",
}


date_bloat_re = re.compile(r"DNES")


def article_transform_fc(tag: Tag):
    if tag.name in ["p", "span", "figcaption", *ALLOWED_H, *TABLE_TAGS, *LIST_TAGS]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name == "div" and len(allowed_classes_div.intersection(classes)) > 0:
        return True

    return False


def date_transform_fallback(fallback: datetime):
    def transform(text: str):
        date = format_date_transform("%d. %m. %Y, %H:%M")(text)
        if date is not None:
            return date

        date = format_date_transform("%H:%M")(text)
        if date is not None:
            return fallback.replace(hour=date.hour, minute=date.minute)
        return None

    return transform


class NovinkyCZExtractor(ArticleExtractor):
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
                "author": "div.ogm-article-author__texts > div:nth-child(2)",
                "category": "div[data-dot='ogm-breadcrumb-navigation'] > a:nth-child(2) > span",
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
            # 2018 version
            **extract_transform(
                soup,
                {"publication_date": "div.ogm-article-author__date"},
                {
                    "publication_date": [
                        get_text_transform,
                        date_transform_fallback(metadata.domain_record.timestamp),
                    ]
                },
            ),
        }
        return extracted_dict


extractor = NovinkyCZExtractor()
