from typing import Any, Callable, Dict
from utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_extract_transform,
    article_transform,
    author_extract_transform,
    extract_date_cz,
    head_extract_transform,
    headline_extract_transform,
    must_exist_filter,
)

head_extract_dict: Dict[str, str] = {
    "headline": "meta[property='og:title']",
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "headline": headline_extract_transform
}


article_extract_dict: Dict[str, Any] = {
    "brief": "header.b-detail__head > p.text-lg",
    "content": "div.b-detail",
    "author": "div.b-detail > p.meta strong",
    "publication_date": "header.b-detail__head > p.meta time",
}

article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": lambda x: article_transform(
        x, fc_eval=lambda x: x.name in [*ALLOWED_H, "p"] and len(x.attrs) == 0
    ),
    "brief": lambda x: x.text if x else None,
    "author": author_extract_transform,
    "publication_date": extract_date_cz
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": "meta[property='og:type']",
}

filter_must_exist: Dict[str, str] = {
    # Prevents Premium "articles"
    "menu": "#menu-main",
}


class Extractor(ArticleExtractor):
    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup.select_one("article[role='article']"),
            article_extract_dict,
            article_extract_transform_dict,
        )

        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        category = metadata.url_parsed.path.split("/")[1]
        extracted_dict["category"] = category
        extracted_dict["keywords"] = None
        extracted_dict["comments_num"] = None

        return extracted_dict

    def filter(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")

        head_extracted = head_extract_transform(soup, filter_head_extract_dict, dict())
        if head_extracted.get("type") != "article":
            return False

        if not must_exist_filter(soup, filter_must_exist):
            return False

        return True
