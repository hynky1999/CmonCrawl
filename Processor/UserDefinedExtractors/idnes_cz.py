from datetime import datetime
import re
from typing import Any, Callable, Dict
from Extractor.extractor_utils import (
    TagDescriptor,
    get_tag_transform,
)
from utils import PipeMetadata
from bs4 import BeautifulSoup, NavigableString, Tag
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    article_extract_transform,
    article_transform,
    author_extract_transform,
    head_extract_transform,
    must_not_exist_filter,
)


head_extract_dict: Dict[str, TagDescriptor] = {
    "headline": TagDescriptor("meta", {"property": "og:title"}),
    "keywords": TagDescriptor("meta", {"name": "keywords"}),
    "publication_date": TagDescriptor("meta", {"property": "article:published_time"}),
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "keywords": lambda x: x.split(","),
    "publication_date": datetime.fromisoformat,
    "headline": lambda x: x.replace(r" - iDNES.cz", "").strip(),
}


article_extract_dict: Dict[str, Any] = {
    "brief": TagDescriptor("div", {"class": "opener"}),
    "content": TagDescriptor("div", {"class": "bbtext"}),
    "author": TagDescriptor("div", {"class": "authors"}),
    "comments_num": TagDescriptor("li", {"class": "community-discusion"}),
}

article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": article_transform,
    "brief": lambda x: x.text if x else None,
    "author": lambda x: author_extract_transform(
        get_tag_transform(x)(TagDescriptor("span", {"itemprop": "name"}))
    ),
    "comments_num": lambda x: idnes_extract_comments_num(x),
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": TagDescriptor("meta", {"property": "og:type"}),
}

filter_must_not_exist: Dict[str, TagDescriptor] = {
    # Prevents Premium "articles"
    "idnes_plus": TagDescriptor("div", {"id": "paywall-unlock"}),
    "brisk": TagDescriptor("span", {"class": "brisk", "text": "Soutěž"}),
}


class Extractor(ArticleExtractor):
    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup.find(attrs={"id": "content"}),
            article_extract_dict,
            article_extract_transform_dict,
        )

        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        category = metadata.url_parsed.path.split("/")[1]
        extracted_dict["category"] = category

        return extracted_dict

    def filter(self, response: str, metadata: PipeMetadata):
        soup = BeautifulSoup(response, "html.parser")

        head_extracted = head_extract_transform(soup, filter_head_extract_dict, dict())
        if head_extracted.get("type") != "article":
            return False

        if not must_not_exist_filter(soup, filter_must_not_exist):
            return False

        return True


def idnes_extract_comments_num(tag: Tag | NavigableString | None):
    if tag is None:
        return None
    comments_num = re.search(r"(\d+) (přísp)", tag.text)
    if comments_num is None:
        return None
    return comments_num.group(1)
