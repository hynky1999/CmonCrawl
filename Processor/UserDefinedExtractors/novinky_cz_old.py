from datetime import datetime
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
)


def article_transform_fc(tag: Tag):
    if tag.name in [*ALLOWED_H]:
        return True

    classes = tag.get("class", [])
    if tag.name != "p" or not isinstance(classes, list) or len(classes) != 0:
        return False

    return True


head_extract_dict: Dict[str, str] = {
    "keywords": "meta[name='keywords']",
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "keywords": lambda x: x.split(","),
}


article_extract_dict: Dict[str, Any] = {
    "content": "div#articleBody",
    "author": "p.articleAuthors",
    "publication_date": "p#articleDate",
    "brief": "p.perex",
}

article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": lambda x: article_transform(
        x,
        fc_eval=article_transform_fc,
    ),
    "author": author_extract_transform,
    "publication_date": extract_date_cz,
    "brief": lambda x: x.text,
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": "meta[property='og:type']",
}

filter_must_exist: Dict[str, str] = {
    # Prevents Premium "articles"
    "menu": "#menu-main",
}


class Extractor(ArticleExtractor):
    UNTIL_DATE = datetime(2019, 8, 6)

    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup.select_one("div#main"),
            article_extract_dict,
            article_extract_transform_dict,
        )

        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        category = metadata.url_parsed.path.split("/")[1]
        extracted_dict["category"] = category
        headline = None
        headline_tag = soup.select_one("head > title")
        if headline_tag is not None:
            headline = headline_extract_transform(headline_tag.text)

        extracted_dict["headline"] = headline
        extracted_dict["comments_num"] = None

        return extracted_dict

    def filter(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")

        return True
