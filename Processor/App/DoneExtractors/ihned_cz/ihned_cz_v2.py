from datetime import datetime
import re
from typing import Any, Dict

from bs4 import BeautifulSoup

from ArticleUtils.article_extractor import ArticleExtractor
from ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    comments_num_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from Extractor.extractor_utils import (
    get_attribute_transform,
    get_tag_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from processor_utils import PipeMetadata

date_sub = re.compile(r"(\(?Poslední aktualizace) | (\(aktualizováno.*)")


class DenikV2Extractor(ArticleExtractor):
    SINCE = datetime(2011, 5, 5)
    TO = datetime(2019, 5, 5)
    ENCODING = "windows-1250"

    def __init__(self):
        super().__init__(
            {
                "headline": "title",
            },
            {
                "headline": [get_text_transform, headline_transform],
            },
            {
                "content": ".article-body",
                "author": ".article-info",
                "comments_num": ".article-info > a[href='#disqus_thread']",
                "keywords": ".tag-list",
            },
            {
                "content": article_content_transform(),
                "author": [
                    get_tags_transform("div > p.author > a"),
                    get_text_list_transform(","),
                    author_transform,
                ],
                "comments_num": [get_text_transform, comments_num_transform],
                "keywords": [
                    get_tags_transform("div > a"),
                    get_text_list_transform(","),
                    keywords_transform,
                ],
            },
            "body",
        )

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        extracted_dict = {
            "publication_date": date_complex_extract(
                soup,
                ".article-info > p.time",
                ["%d. %m. %Y %H:%M", "%d. %m. %Y"],
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
                custom_cleaner=lambda x: date_sub.sub("", x),
            ),
            "brief": self.custom_brief_extract(soup),
            "category": None,
        }
        return extracted_dict

    def custom_brief_extract(self, soup: BeautifulSoup):
        brief_tag = get_tag_transform(".headlines")(soup)
        if brief_tag is not None:
            brief = get_text_transform(brief_tag)
            brief = brief_transform(brief)
            if brief != "":
                return brief

        brief_tag = get_tag_transform("head > meta[name='description']")(soup)
        if brief_tag is not None:
            brief = get_attribute_transform("content")(brief_tag)
            brief = brief_transform(brief)
            if brief != "":
                return brief
        return None


extractor = DenikV2Extractor()
