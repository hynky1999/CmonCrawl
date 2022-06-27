from datetime import datetime
from typing import Any, Callable, Dict
from Processor.Extractor.extractor_utils import (
    TagDescriptor,
)
from Processor.utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from article_utils.article_extractor import ArticleExtractor

from article_utils.article_utils import (
    article_extract_transform,
    article_transform,
    head_extract_transform,
)


def author_extract_transform(author: Tag):
    if author is None:
        return None

    text = author.text.strip()
    # Not name
    if len(text.split(" ")) < 2:
        return None

    return text


head_extract_dict: Dict[str, TagDescriptor] = {
    "headline": TagDescriptor("meta", {"property": "og:title"}),
    "keywords": TagDescriptor("meta", {"name": "keywords"}),
    "publication_date": TagDescriptor("meta", {"property": "article:published_time"}),
    "category": TagDescriptor("meta", {"property": "article:section"}),
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "keywords": lambda x: x.split(","),
    "publication_date": datetime.fromisoformat,
    "headline": lambda x: x.replace(r" | Aktuálně.cz", "").strip(),
}


article_extract_dict: Dict[str, Any] = {
    "brief": TagDescriptor("div", {"class": "article__perex"}),
    "content": TagDescriptor("div", {"class": "article__content"}),
    "author": TagDescriptor("a", {"class": "author__name"}),
}


article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": article_transform,
    "brief": lambda x: x.text if x else None,
    "author": author_extract_transform,
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": TagDescriptor("meta", {"property": "og:type"}),
}

filter_must_exist: Dict[str, TagDescriptor] = {
    # Prevents Aktualne+ "articles"
    "aktualne_upper_menu": TagDescriptor("div", {"id": "aktu-menu-spa"}),
}

filter_allowed_domain_prefixes = ["zpravy", "nazory", "sport", "magazin"]


class Extractor(ArticleExtractor):
    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup, article_extract_dict, article_extract_transform_dict
        )
        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        extracted_dict["comments_num"] = None
        metadata.article_data = extracted_dict

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

        must_exist = [
            soup.find(tag_desc.tag, **tag_desc.attrs)
            for tag_desc in filter_must_exist.values()
        ]
        if any(map(lambda x: x is None, must_exist)):
            return False

        return True
