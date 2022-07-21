import json
from typing import Any, Dict
from bs4 import BeautifulSoup, Tag
from Extractor.extractor_utils import (
    extract_transform,
    get_attribute_transform,
    get_tag_transform,
    get_text_transform,
)
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    ALLOWED_H,
    LIST_TAGS,
    TABLE_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    date_complex_extract,
    format_date_transform,
    headline_transform,
    keywords_transform,
)
from utils import PipeMetadata
from DoneExtractors.aktualne_cz.aktualne_cz_v1 import year_since_to_re

allowed_classes_div = {
    # text
    "g_aa",
    "d_ch",
    "g_V",
    "f_c5",
    "f_c6",
}

allowed_classes_figure = {
    # image
    "g_bu",
    "f_cN",
    "d_bD",
}


def article_fc(tag: Tag):
    if tag.name in [*ALLOWED_H, "span", "p", *TABLE_TAGS, *LIST_TAGS]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name == "div" and len(allowed_classes_div.intersection(classes)) > 0:
        return True

    if tag.name == "figure" and len(allowed_classes_figure.intersection(classes)) > 0:
        return True
    return False


def date_transform_script(text: str):
    try:
        json_data = json.loads(text)
    except json.decoder.JSONDecodeError:
        return None
    date_str = json_data.get("datePublished", "")
    return format_date_transform("%Y-%m-%dT%H:%M:%S.%fZ")(date_str)


class SeznamZpravyExtractor(ArticleExtractor):
    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "brief": "meta[property='og:description']",
                "keywords": "meta[name='keywords']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
                "keywords": [get_attribute_transform("content"), keywords_transform],
            },
            {
                "content": "article > div[data-dot='ogm-article-content'] > div",
                "author": "[data-dot='mol-author-names']",
                "category": "div[data-dot='ogm-breadcrumb-navigation'] > a:nth-child(2) > span",
            },
            {
                "content": [
                    article_content_transform(fc_eval=article_fc),
                    lambda x: x.replace(
                        "Článek si také můžete poslechnout v audioverzi.\n", ""
                    ),
                ],
                "author": [get_text_transform, author_transform],
                "category": [get_text_transform, category_transform],
            },
            "main[role='main']",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict: Dict[str, Any] = {
            "comments_num": None,
            "publication_date": self.custom_extract_date(soup, metadata),
            # 2018 version
            **extract_transform(
                soup,
                {"category": ".ogm-header-toggle-button-text"},
                {"category": [get_text_transform, category_transform]},
            ),
        }
        return extracted_dict

    def custom_extract_date(self, soup: BeautifulSoup, metadata: PipeMetadata):
        script_tag = get_tag_transform(
            "div[class*='document-type'] > script[type='application/ld+json']"
        )(soup)
        date = None
        if script_tag is not None:
            date = date_transform_script(get_text_transform(script_tag))
        if date is not None:
            return date
        date = date_complex_extract(
            soup,
            ["div[data-dot='ogm-date-of-publication']", "span.atm-date-formatted"],
            "%d. %m. %Y %H:%M",
            year_css=".main-footer footer",
            year_regex=year_since_to_re,
            no_year_date_format="%d. %m. %H:%M",
            hours_minutes_with_fallback=True,
            fallback=metadata.domain_record.timestamp,
        )
        return date


extractor = SeznamZpravyExtractor()
