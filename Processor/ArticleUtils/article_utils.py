from datetime import datetime
import logging
import re
from typing import Any, Callable, Dict
from urllib.parse import ParseResult
from xmlrpc.client import Boolean

from bs4 import BeautifulSoup, NavigableString, Tag

from Extractor.extractor_utils import (
    all_same_transform,
    get_attribute_transform,
    get_tag_transform,
    transform,
)

LINE_SEPARATOR = "\n"


# All keys are required if value if true then the extracted value must be non None
REQUIRED_FIELDS = {
    "content": True,
    "headline": True,
    "author": True,
    "brief": False,
    "publication_date": False,
    "keywords": False,
    "category": False,
    "comments_num": False,
}

ALLOWED_H = [f"h{i}" for i in range(1, 7)]


def article_transform(article: Tag, fc_eval: Callable[[Tag], Boolean] | None = None):
    if article is None:
        return None

    if fc_eval is None:
        ps = article.find_all(["p", *ALLOWED_H], recursive=False)
    else:
        ps = article.find_all(fc_eval, recursive=False)
    texts = LINE_SEPARATOR.join([p.text for p in ps])
    return texts


def text_unification_transform(text: Any):

    if isinstance(text, str):
        text = text.strip().replace("\xa0", " ")
    elif isinstance(text, list):
        text = list(map(text_unification_transform, text))

    return text


def head_extract_transform(
    soup: BeautifulSoup,
    head_extract_dict: Dict[str, str],
    head_extract_transform_dict: Dict[str, Callable[[str], Any]],
) -> Dict[str, Any]:
    head = soup.select_one("head")
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
        ),
        all_same_transform(head_extract_dict, text_unification_transform),
    )

    return extracted_metadata


def article_extract_transform(
    soup: BeautifulSoup | Tag | NavigableString | None,
    article_extract_dict: Dict[str, str],
    article_extract_transform_dict: Dict[str, Callable[[Tag], Any]],
) -> Dict[str, Any]:
    if soup is None:
        return dict()

    article_data = transform(
        transform(
            transform(
                article_extract_dict,
                all_same_transform(article_extract_dict, get_tag_transform(soup)),
            ),
            article_extract_transform_dict,
        ),
        all_same_transform(article_extract_dict, text_unification_transform),
    )
    return article_data


def author_extract_transform(author: Tag | None | NavigableString):
    if author is None:
        return None

    text = author.text.strip()

    return text


def headline_extract_transform(headline: str):
    headline = re.split(r"[-–]", headline)[0]
    return text_unification_transform(headline)


def must_exist_filter(soup: BeautifulSoup, filter_dict: Dict[str, Any]):
    must_exist = [
        soup.select_one(css_selector) for css_selector in filter_dict.values()
    ]
    if any(map(lambda x: x is None, must_exist)):
        return False

    return True


def must_not_exist_filter(soup: BeautifulSoup, filter_dict: Dict[str, Any]):
    must_not_exist = [
        soup.select_one(css_selector) for css_selector in filter_dict.values()
    ]
    if any(map(lambda x: x is not None, must_not_exist)):
        return False

    return True


def extract_publication_date(format: str):
    def inner(tag: Tag | NavigableString | None):
        if tag is None:
            return None
        try:
            return datetime.strptime(tag.text, format)
        except ValueError:
            pass
        return None

    return inner


def extract_category_from_url(url: ParseResult):
    category_split = str(url).split("/")
    category = None
    if len(category_split) > 1:
        category = category_split[1]
    return category


def extract_date_cz(date_tag: Tag | None):
    if date_tag is None:
        return None
    date_str = date_tag.text
    try:
        date_match = re.search(r"(\d{1,2})\.?\s+(\w+)\s+(\d{4})", date_str)
        if date_match is None:
            logging.error(f"Invalid CZ date: {date_str}")
            return None
        day, month, year = date_match.groups()

        month = CZ_EN_MONTHS.index(month) + 1
        date_iso = f"{year}-{month:02}-{int(day):02}"
        return datetime.fromisoformat(date_iso)
    except ValueError:
        logging.error(f"Invalid CZ date: {date_str}")
        return None


CZ_EN_MONTHS = [
    "ledna",
    "února",
    "března",
    "dubna",
    "května",
    "června",
    "července",
    "srpna",
    "září",
    "října",
    "listopadu",
    "prosince",
]
