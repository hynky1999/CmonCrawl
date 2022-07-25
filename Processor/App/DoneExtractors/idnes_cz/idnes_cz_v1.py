from datetime import datetime
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
from Processor.App.ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    comments_num_transform,
    cz_date_transform,
    format_date_transform,
    headline_transform,
    keywords_transform,
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)


def date_transform(text: str):
    date = format_date_transform("%d. %m. %Y %H:%M")(text)
    if date is not None:
        return date
    date = format_date_transform("%d. %m. %Y")(text)
    if date is not None:
        return date
    return None


class IdnesCZV1Extractor(ArticleExtractor):
    ENCODING = "windows-1250"
    TO = datetime(2011, 8, 9)

    def __init__(self):
        super().__init__(
            {
                "headline": "title",
                "keywords": "meta[name='keywords']",
                "brief": "meta[name='description']",
            },
            {
                "headline": [get_text_transform, headline_transform],
                "keywords": [get_attribute_transform("content"), keywords_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
            },
            {
                "content": "div.text > div.bbtext",
                "category": "li.act > a",
                "author": "div.authors > ul",
                "publication_date": "div.art-info > span.time",
                "comments_num": "div.btm",
            },
            {
                "content": article_content_transform(),
                "author": [
                    lambda x: get_tags_transform("li a")(x)
                    + get_tags_transform("li span.name")(x),
                    get_text_list_transform(","),
                    author_transform,
                ],
                "publication_date": [
                    get_text_transform,
                    cz_date_transform,
                    date_transform,
                ],
                "comments_num": [
                    get_text_transform,
                    lambda x: x.split(",")[0],
                    comments_num_transform,
                ],
                "category": [get_text_transform, category_transform],
            },
            "#main",
        )


extractor = IdnesCZV1Extractor()
