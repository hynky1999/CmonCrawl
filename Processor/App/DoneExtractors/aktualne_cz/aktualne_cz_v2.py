from datetime import datetime
import re
from typing import Any, Dict
from Processor.App.ArticleUtils.article_utils import (
    ALLOWED_H,
    LIST_TAGS,
    TABLE_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tag_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from Processor.App.processor_utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor


allowed_classes_p = {"p1"}


def article_fc(tag: Tag):
    if tag.name in [*LIST_TAGS, "figure", *TABLE_TAGS]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name in ["p", *ALLOWED_H] and (
        len(classes) == 0 or len(allowed_classes_p.intersection(classes)) > 0
    ):
        return True

    return False


year_since_to_re = re.compile(r"\d{4}\s*–\s*(?P<year>\d{4})")


class AktualneCZV2Extractor(ArticleExtractor):
    SINCE = datetime(2014, 9, 10)
    TO = datetime(2019, 9, 10)

    def __init__(self):

        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "keywords": "meta[name='keywords']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
                "keywords": [get_attribute_transform("content"), keywords_transform],
            },
            {
                "category": ".hlavicka a.active",
                "brief": ".perex",
            },
            {
                "brief": [get_text_transform, brief_transform],
                "category": [get_text_transform, category_transform],
            },
            "body",
        )

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        extracted_dict = {
            "publication_date": date_complex_extract(
                soup,
                ".titulek-pubtime",
                ["%d. %m. %Y %H:%M", "%d. %m. %Y"],
                no_year_date_format="%d. %m. %H:%M",
                year_css="#copyright-utm",
                year_regex=year_since_to_re,
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
            ),
            "content": self.custom_content_extract(soup, metadata),
            "comments_num": None,
            "author": self.custom_author_extract(soup, metadata),
        }
        return extracted_dict

    def custom_filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        parsed = metadata.url_parsed.path.split("/")
        if len(parsed) > 1 and parsed[1] in ["wiki"]:
            return False
        return True

    def custom_content_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        tag = get_tag_transform("div[data-ec-list*='zpravy']")(soup)
        if tag is not None:
            return article_content_transform(fc_eval=article_fc)(tag)

        tag = get_tag_transform("div[data-upscore='article']")(soup)
        if tag is not None:
            return article_content_transform(fc_eval=article_fc)(tag)

        tag = get_tag_transform("div.clanek")(soup)
        if tag is not None:
            return article_content_transform(fc_eval=article_fc)(tag)

        return None

    def custom_author_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        tag = get_tag_transform("p[class*=clanek-autor]")(soup)
        if tag is not None:
            autors_tags = get_tags_transform("a.autor")(tag)
            if len(autors_tags) > 0:
                autors_str = get_text_list_transform(",")(autors_tags)
            else:
                autors_str = get_text_transform(tag)
            if autors_str is not None:
                return author_transform(autors_str)
        return None


extractor = AktualneCZV2Extractor()
