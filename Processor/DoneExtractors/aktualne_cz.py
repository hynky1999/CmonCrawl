from datetime import datetime
from typing import Any, Dict
from ArticleUtils.article_utils import (
    ALLOWED_H,
    LIST_TAGS,
    TABLE_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    headline_transform,
    iso_date_transform,
    keywords_transform,
)
from Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from ArticleUtils.article_extractor import ArticleExtractor


allowed_classes_div = {
    # text
    "article__photo",
    "infobox",
}


def article_fc(tag: Tag):
    if tag.name in [*LIST_TAGS, "figure", *TABLE_TAGS]:
        return True

    if tag.name in ["p", *ALLOWED_H]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name == "div" and len(allowed_classes_div.intersection(classes)) > 0:
        return True

    return False


class AktualneCZExtractor(ArticleExtractor):
    SINCE = datetime(2019, 9, 10)

    def __init__(self):

        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "keywords": "meta[name='keywords']",
                "publication_date": "meta[property='article:published_time']",
                "category": "meta[property='article:section']",
                "brief": "meta[property='og:description']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
                "keywords": [get_attribute_transform("content"), keywords_transform],
                "publication_date": [
                    get_attribute_transform("content"),
                    iso_date_transform,
                ],
                "category": [get_attribute_transform("content"), category_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
            },
            {
                "content": "div.article__content",
                "author": "div.author",
            },
            {
                "content": article_content_transform(article_fc),
                "brief": [get_text_transform, brief_transform],
                "author": [
                    get_tags_transform("div > a.author__name"),
                    get_text_list_transform(","),
                    author_transform,
                ],
            },
            "div.left-column > div.article",
            filter_must_exist=[
                # Prevents Aktualne+ "articles"
                "div[id='aktu-menu-spa']",
            ],
            filter_allowed_domain_prefixes=[
                "zpravy",
                "nazory",
                "sport",
                "magazin",
                "zena",
            ],
        )

    def custom_filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        parsed = metadata.url_parsed.path.split("/")
        if len(parsed) > 1 and parsed[1] in ["wiki"]:
            return False
        return True

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        return {"comments_num": None}


extractor = AktualneCZExtractor()
