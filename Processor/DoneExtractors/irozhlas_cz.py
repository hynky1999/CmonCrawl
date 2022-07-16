from typing import Any, Callable, Dict
from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_content_transform,
    author_transform,
    brief_transform,
    cz_date_transform,
    format_date_transform,
    headline_transform,
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


article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {}


filter_head_extract_dict: Dict[str, Any] = {
    "type": "meta[property='og:type']",
}

filter_must_exist: Dict[str, str] = {
    # Prevents Premium "articles"
    "menu": "#menu-main",
}


class IrozhlasExtractor(ArticleExtractor):
    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "brief": "meta[property='og:description']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
            },
            {
                "content": "div.b-detail",
                "author": "div.b-detail > p.meta strong",
                "publication_date": "header.b-detail__head > p.meta time",
                "keywords": "nav.m-breadcrumb",
                "category": "nav.m-breadcrumb > a:nth-child(3)",
            },
            {
                "content": lambda x: article_content_transform(
                    x,
                    fc_eval=lambda x: x.name in [*ALLOWED_H, "p"] and len(x.attrs) == 0,
                ),
                "author": [get_text_transform, author_transform],
                "publication_date": [
                    get_text_transform,
                    cz_date_transform,
                    format_date_transform("%H:%M %d. %m. %Y"),
                ],
                "keywords": [
                    get_tags_transform(
                        "nav > a[href*='zpravy-tag']",
                    ),
                    get_text_list_transform(","),
                    keywords_transform,
                ],
                "category": [get_text_transform, brief_transform],
            },
            "main#main",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        return {"comments_num": None}


extractor = IrozhlasExtractor()
