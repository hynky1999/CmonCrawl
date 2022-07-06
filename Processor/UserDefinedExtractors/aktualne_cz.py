from datetime import datetime
from typing import Any, Callable, Dict
from utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    article_extract_transform,
    article_transform,
    author_extract_transform,
    head_extract_transform,
    must_exist_filter,
)


head_extract_dict: Dict[str, str] = {
    "headline": "meta[property='og:title']",
    "keywords": "meta[name='keywords']",
    "publication_date": "meta[property='article:published_time']",
    "category": "meta[property='article:section']",
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "keywords": lambda x: x.split(","),
    "publication_date": datetime.fromisoformat,
    "headline": lambda x: x.replace(r" | Aktuálně.cz", "").strip(),
}


article_extract_dict: Dict[str, Any] = {
    "brief": "div[class='article__perex']",
    "content": "div[class='article__content']",
    "author": "a[class='author__name']",
}


article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": article_transform,
    "brief": lambda x: x.text if x else None,
    "author": author_extract_transform,
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": "meta[property='og:type']",
}

filter_must_exist: Dict[str, str] = {
    # Prevents Aktualne+ "articles"
    "aktualne_upper_menu": "div[id='aktu-menu-spa']",
}

filter_allowed_domain_prefixes = ["zpravy", "nazory", "sport", "magazin"]


class Extractor(ArticleExtractor):
    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup.select_one("div.page"),
            article_extract_dict,
            article_extract_transform_dict,
        )
        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        extracted_dict["comments_num"] = None

        return extracted_dict

    def filter(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")

        if (
            metadata.url_parsed
            and metadata.url_parsed.netloc.split(".")[0]
            not in filter_allowed_domain_prefixes
        ):
            return False

        head_extracted = head_extract_transform(soup, filter_head_extract_dict, dict())
        if head_extracted["type"] != "article":
            return False

        if not must_exist_filter(soup, filter_must_exist):
            return False

        return True
