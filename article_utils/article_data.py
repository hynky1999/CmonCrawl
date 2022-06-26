from dataclasses import dataclass
from datetime import datetime
from typing import List

from bs4 import Tag

LINE_SEPARATOR = "\n"


@dataclass
class ArticleData:
    content: str
    headline: str | None = None
    brief: str | None = None
    author: str | None = None
    publication_date: datetime | None = None
    keywords: List[str] | None = None
    category: str | None = None
    comments_num: int | None = None


def article_transform(article: Tag):
    if article is None:
        return None

    ps = article.find_all("p", recursive=False)
    texts = LINE_SEPARATOR.join([p.text for p in ps])
    return texts
