from datetime import datetime
import re
from typing import Any, Callable, Dict, List
from urllib.parse import ParseResult

from bs4 import BeautifulSoup, Tag

LINE_SEPARATOR = "\n"

ALLOWED_H = [f"h{i}" for i in range(1, 7)]


def text_unifications_transform(text: List[str]):
    unified = list(map(text_unification_transform, text))
    return [x for x in unified if x != ""]


def text_unification_transform(text: str):
    return text.strip().replace("\xa0", " ")


def article_content_transform(fc_eval: Callable[[Tag], bool] | None = None):
    def transform(article: Tag):
        if fc_eval is None:
            ps = article.find_all(["p", *ALLOWED_H], recursive=False)
        else:
            ps = article.find_all(fc_eval, recursive=False)
        texts = LINE_SEPARATOR.join([text_unification_transform(p.text) for p in ps])
        return text_unification_transform(texts)

    return transform


def author_transform(author: str):
    authors = author.split(",")
    authors = text_unifications_transform(authors)
    return authors


def headline_transform(headline: str):
    headline = re.split(r"[-–]", headline)[0]
    headline = re.split(r"[|]", headline)[0]
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


def format_date_transform(format: str):
    def inner(text: str):
        date = None
        try:
            text_unif = text_unification_transform(text)
            date = datetime.strptime(text_unif, format)
        except ValueError:
            pass
        return date

    return inner


def url_category_transform(url: ParseResult):
    category_split = str(url).split("/")
    category = None
    if len(category_split) > 1:
        category = category_split[1]
    else:
        return None
    return text_unification_transform(category)


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
CZ_month_sub = re.compile("|".join(CZ_EN_MONTHS))


def cz_date_transform(date_str: str):
    return CZ_month_sub.sub(
        lambda x: f"{CZ_EN_MONTHS.index(x.group(0)) + 1}.", date_str
    )


DAYS = ["pondělí", "úterý", "středa", "čtvrtek", "pátek", "sobota", "neděle"]
DAYS_SUB = re.compile("|".join(DAYS))


def remove_day_transform(date_str: str):
    return DAYS_SUB.sub("", date_str)


def keywords_transform(text: str):
    keywords_splitted = text.split(",")
    return text_unifications_transform(keywords_splitted)


brief_sub = re.compile(r"EXKLUZIVNĚ\.")


def brief_transform(text: str):
    brief_text = brief_sub.sub("", text)
    return text_unification_transform(brief_text)


def comments_num_transform(text: str):
    comments_num = re.search(r"\d+", text)
    if comments_num is None:
        return None
    return comments_num.group(0)


def category_transform(category: str):
    return text_unification_transform(category)
