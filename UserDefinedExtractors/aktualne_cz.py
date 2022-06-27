from datetime import datetime
from typing import Any, Callable, Dict
from Processor.Extractor.extractor import BaseExtractor
from Processor.Extractor.extractor_utils import (
    TagDescriptor,
    all_same_transform,
    get_attribute_transform,
    get_tag_transform,
    transform,
)
from Processor.utils import PipeMetadata
from bs4 import BeautifulSoup, NavigableString, Tag

from article_utils.article_data import (
    ArticleData,
    article_transform,
    text_unification_transform,
)


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
    "brief": lambda x: x.text,
    "author": lambda x: x.text,
}


class Extractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata) -> str:
        soup = BeautifulSoup(response, "html.parser")
        extracted_metadata = self.extract_metadata(soup)
        extracted_article = self.extract_article(soup)
        # merge dicts
        extracted_dict = {**extracted_metadata, **extracted_article}
        metadata.article_data = ArticleData(**extracted_dict)
        return metadata.article_data.content

    def extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        head = soup.find("head")
        if head is None or isinstance(head, NavigableString):
            return dict()

        extracted_metadata: Dict[str, Any] = transform(
            transform(
                transform(
                    transform(
                        head_extract_dict,
                        all_same_transform(head_extract_dict, get_tag_transform(head)),
                    ),
                    all_same_transform(
                        head_extract_dict, get_attribute_transform("content")
                    ),
                ),
                head_extract_transform_dict,
            ), all_same_transform(head_extract_dict, text_unification_transform)
        )

        return extracted_metadata

    def extract_article(self, soup: BeautifulSoup) -> Dict[str, Any]:
        article = soup.find("div", attrs={"class": "article"})
        if article is None or isinstance(article, NavigableString):
            return dict()
        article_data = transform(
            transform(
                transform(
                    article_extract_dict,
                    all_same_transform(
                        article_extract_dict, get_tag_transform(article)
                    ),
                ),
                article_extract_transform_dict,
            ),
            all_same_transform(article_extract_dict, text_unification_transform),
        )
        return article_data
