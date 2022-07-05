from datetime import datetime
from typing import Any, Callable, Dict
from Extractor.extractor_utils import (
    TagDescriptor,
    get_tag_transform,
)
from utils import PipeMetadata
from bs4 import BeautifulSoup, NavigableString, Tag
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_extract_transform,
    article_transform,
    author_extract_transform,
    head_extract_transform,
)


def seznamzpravy_extract_publication_date(tag: Tag | NavigableString | None):
    if tag is None:
        return None
    try:
        return datetime.strptime(tag.text, "%d. %m. %Y %H:%M")
    except ValueError:
        pass
    return None


head_extract_dict: Dict[str, TagDescriptor] = {
    "headline": TagDescriptor("meta", {"property": "og:title"}),
    "keywords": TagDescriptor("meta", {"name": "keywords"}),
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "keywords": lambda x: x.split(","),
    "headline": lambda x: x.replace(r" - iDNES.cz", "").strip(),
}


article_extract_dict: Dict[str, Any] = {
    "brief": TagDescriptor("p", {"data-dot": "ogm-article-perex"}),
    "content": TagDescriptor("div", {"class": "mol-rich-content--for-article"}),
    "author": TagDescriptor("div", {"data-dot": "ogm-article-author"}),
    "publication_date": TagDescriptor(
        "div", {"data-dot": "ogm-date-of-publication__date"}
    ),
}

article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": lambda x: article_transform(
        x,
        fc_eval=lambda x: x.name in ALLOWED_H
        or (x.name == "div" and x.attrs.get("data-dot", "") == "mol-paragraph"),
    ),
    "brief": lambda x: x.text if x else None,
    "author": lambda x: author_extract_transform(
        get_tag_transform(x)(TagDescriptor("span", {"data-dot": "mol-author-names"}))
    ),
    "publication_date": seznamzpravy_extract_publication_date,
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": TagDescriptor("meta", {"property": "og:type"}),
}


class Extractor(ArticleExtractor):
    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup.find("section", attrs={"data-dot": "tpl-content"}),
            article_extract_dict,
            article_extract_transform_dict,
        )

        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        extracted_dict["category"] = None
        extracted_dict["comments_num"] = None

        return extracted_dict

    def filter(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")

        head_extracted = head_extract_transform(soup, filter_head_extract_dict, dict())
        if head_extracted.get("type") != "article":
            return False

        return True
