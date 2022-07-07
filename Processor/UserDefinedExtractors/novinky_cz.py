from typing import Any, Callable, Dict
from utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from ArticleUtils.article_extractor import ArticleExtractor
from UserDefinedExtractors.novinky_cz_old import Extractor as OldExtractor

from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_extract_transform,
    article_transform,
    author_extract_transform,
    extract_publication_date,
    head_extract_transform,
    headline_extract_transform,
)


def article_transform_fc(tag: Tag):
    if tag.name in ALLOWED_H:
        return True

    if tag.get("data-dot", None) is not None:
        return False

    classes = tag.get("class", [])
    if not isinstance(classes, list) or len(classes) != 1:
        return False

    return classes[0].startswith("g_c")


head_extract_dict: Dict[str, str] = {
    "headline": "meta[property='og:title']",
    "keywords": "meta[name='keywords']",
    "brief": "meta[property='og:description']",
}


head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
    "headline": headline_extract_transform,
    "keywords": lambda x: x.split(","),
}


article_extract_dict: Dict[str, Any] = {
    "content": 'article > div[data-dot-data=\'{"component":"article-content"}\'] > div',
    "author": "div.ogm-article-author__texts > div:nth-child(2)",
    "publication_date": "div.ogm-article-author__date > span",
}

article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
    "content": lambda x: article_transform(
        x,
        fc_eval=article_transform_fc,
    ),
    "author": author_extract_transform,
    "publication_date": extract_publication_date("%d. %m. %Y, %H:%M"),
}


filter_head_extract_dict: Dict[str, Any] = {
    "type": "meta[property='og:type']",
}


class Extractor(ArticleExtractor):
    def __init__(self):
        self.old_extractor = OldExtractor()

    def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        if metadata.domain_record.timestamp <= self.old_extractor.UNTIL_DATE:
            return self.old_extractor.article_extract(soup, metadata)

        extracted_head = head_extract_transform(
            soup, head_extract_dict, head_extract_transform_dict
        )

        extracted_article = article_extract_transform(
            soup.select_one("main[role='main']"),
            article_extract_dict,
            article_extract_transform_dict,
        )

        # merge dicts
        extracted_dict = {**extracted_head, **extracted_article}
        category = metadata.url_parsed.path.split("/")[1]
        extracted_dict["category"] = category
        extracted_dict["comments_num"] = None

        return extracted_dict

    def filter(self, response: str, metadata: PipeMetadata):
        if metadata.domain_record.timestamp <= OldExtractor.UNTIL_DATE:
            return self.old_extractor.filter(response, metadata)

        soup = BeautifulSoup(response, "html.parser")
        return True
