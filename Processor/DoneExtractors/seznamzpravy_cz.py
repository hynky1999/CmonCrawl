from datetime import datetime
import json
import re
from typing import Any, Dict
from bs4 import BeautifulSoup, Tag
from Extractor.extractor_utils import (
    extract_transform,
    get_attribute_transform,
    get_text_transform,
)
from ArticleUtils.article_extractor import ArticleExtractor

from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    format_date_transform,
    headline_transform,
    keywords_transform,
)
from utils import PipeMetadata

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
    if tag.name in [*ALLOWED_H]:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name == "div" and len(allowed_classes_div.intersection(classes)) > 0:
        return True

    if tag.name == "figure" and len(allowed_classes_figure.intersection(classes)) > 0:
        return True
    return False


def date_transform_no_script(fallback: datetime):
    def transform(text: str):
        date = format_date_transform("%d. %m. %Y %H:%M")(text)
        if date is not None:
            return date

        date = format_date_transform("%d. %m. %H:%M")(text)
        if date is not None:
            # so that we know that year is not correct
            return date.replace(year=1)

        date = format_date_transform("%H:%M")(text)
        if date is not None:
            return fallback.replace(hour=date.hour, minute=date.minute)

        return None

    return transform


def date_transform_script(text: str):
    try:
        json_data = json.loads(text)
    except json.decoder.JSONDecodeError:
        return None
    date_str = json_data.get("datePublished", "")
    return format_date_transform("%Y-%m-%dT%H:%M:%S.%fZ")(date_str)


year_re = re.compile(r"Copyright © \d{4}-(\d{4})")


def year_transform(text: str):
    year = year_re.search(text)
    if year is not None:
        try:
            return int(year.group(1))
        except ValueError:
            pass
    return None


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
        e_dict = extract_transform(
            soup,
            {
                "date_1": "div[data-dot='ogm-date-of-publication']",
                "date_2": "span.atm-date-formatted",
                "date_script": "div[class*='document-type'] > script[type='application/ld+json']",
                "copyright_year": ".main-footer footer",
            },
            {
                "date_1": [
                    get_text_transform,
                    date_transform_no_script(metadata.domain_record.timestamp),
                ],
                "date_2": [get_text_transform, format_date_transform("%d. %m. %Y")],
                "date_script": [get_text_transform, date_transform_script],
                "copyright_year": [get_text_transform, year_transform],
            },
        )
        date = None
        if e_dict is None:
            return None

        if e_dict["date_script"] is not None:
            date = e_dict["date_script"]

        elif e_dict["date_1"] is not None:
            date = e_dict["date_1"]
            if date.year == 1:
                year = (
                    e_dict["copyright_year"]
                    if e_dict["copyright_year"] is not None
                    else metadata.domain_record.timestamp.year
                )
                date = date.replace(year=year)
        elif e_dict["date_2"] is not None:
            date = e_dict["date_2"]
        return date


extractor = SeznamZpravyExtractor()
