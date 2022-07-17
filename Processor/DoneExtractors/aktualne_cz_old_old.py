from datetime import datetime
import re
from typing import Any, Dict
from ArticleUtils.article_utils import (
    ALLOWED_H,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    comments_num_transform,
    format_date_transform,
    headline_transform,
    keywords_transform,
)
from Extractor.extractor_utils import (
    extract_transform,
    get_attribute_transform,
    get_text_transform,
)
from utils import PipeMetadata
from bs4 import BeautifulSoup, Tag
from ArticleUtils.article_extractor import ArticleExtractor


allowed_classes_div = {
    # text
    "box-clanek",
}


def article_fc(tag: Tag):
    if tag.name in ["p", *ALLOWED_H] and tag.get("class") is None:
        return True

    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = [classes]
    if tag.name == "div" and len(allowed_classes_div.intersection(classes)) > 0:
        return True

    return False


def date_transform_no_script(fallback: datetime):
    def transform(text: str):
        date = format_date_transform("%d. %m. %Y %H:%M")(text)
        if date is not None:
            return date

        date = format_date_transform("%d. %m. %Y")(text)
        if date is not None:
            return date

        date = format_date_transform("%d. %m. %H:%M")(text)
        if date is not None:
            # so that we know that year is not correct
            return date.replace(year=1)

        # Aktualizvoáno nebo dnes
        return fallback

    return transform


year_re = re.compile(r"\d{4}\s*–\s*(\d{4})")


def year_transform(text: str):
    year = year_re.search(text)
    if year is not None:
        try:
            return int(year.group(1))
        except ValueError:
            pass
    return None


class AktualneCZOldOldExtractor(ArticleExtractor):
    TO = datetime(2015, 9, 10)

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
                "content": "div.clanek-text-obsah",
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
                    lambda x: x.replace("autor:", ""),
                    author_transform,
                ],
                "comments_num": [get_text_transform, comments_num_transform],
                "category": [get_text_transform, category_transform],
            },
            "div#obsah",
            filter_allowed_domain_prefixes=[
                "zpravy",
                "nazory",
                "sport",
                "magazin",
                "zena",
            ],
        )
        self.date_css = ".clanek-datum"

    def custom_extract(
        self, soup: BeautifulSoup, metadata: PipeMetadata
    ) -> Dict[str, Any]:
        extracted_dict = {"publication_date": self.custom_extract_date(soup, metadata)}
        return extracted_dict

    def custom_filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        parsed = metadata.url_parsed.path.split("/")
        if len(parsed) > 1 and parsed[1] in ["wiki"]:
            return False
        return True

    def custom_extract_date(self, soup: BeautifulSoup, metadata: PipeMetadata):
        e_dict = extract_transform(
            soup,
            {
                "date": self.date_css,
                "copyright_year": "#paticka-utm",
            },
            {
                "date": [
                    get_text_transform,
                    date_transform_no_script(metadata.domain_record.timestamp),
                ],
                "copyright_year": [get_text_transform, year_transform],
            },
        )
        date = None
        if e_dict is None:
            return None

        if e_dict["date"] is not None:
            date = e_dict["date"]
            if date.year == 1:
                year = (
                    e_dict["copyright_year"]
                    if e_dict["copyright_year"] is not None
                    else metadata.domain_record.timestamp.year
                )
                date = date.replace(year=year)
        return date


extractor = AktualneCZOldOldExtractor()
