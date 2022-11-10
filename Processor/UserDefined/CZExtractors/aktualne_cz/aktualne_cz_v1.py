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
    comments_num_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
    year_since_to_re,
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_text_transform,
)
from Processor.App.processor_utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor


def article_fc(tag: Tag):
    if tag.name in [*LIST_TAGS, "figure", *TABLE_TAGS]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name in ["p", *ALLOWED_H] and len(classes) == 0:
        return True

    return False




class AktualneCZV1Extractor(ArticleExtractor):
    TO = datetime(2014, 9, 10)

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
                "content": "div[class*=clanek-text]",
                "category": "ul#nav-breadcrumb > li >  a",
                "author": "p.clanek-autor",
                "comments_num": "#disqusCounter > span",
                "brief": ".clanek-perex",
            },
            {
                "content": article_content_transform(fc_eval=article_fc),
                "brief": [get_text_transform, brief_transform],
                "author": [
                    get_text_transform,
                    author_transform,
                ],
                "comments_num": [get_text_transform, comments_num_transform],
                "category": [get_text_transform, category_transform],
            },
            "div#obsah",
        )
        self.date_css = ".clanek-datum"

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        extracted_dict = {
            "publication_date": date_complex_extract(
                soup,
                self.date_css,
                ["%d. %m. %Y %H:%M", "%d. %m. %Y"],
                no_year_date_format="%d. %m. %H:%M",
                year_css="#paticka-utm",
                year_regex=year_since_to_re,
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
            )
        }
        return extracted_dict

    def custom_filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        parsed = metadata.url_parsed.path.split("/")
        if len(parsed) > 1 and parsed[1] in ["wiki"]:
            return False
        return True


extractor = AktualneCZV1Extractor()
