from datetime import datetime

from bs4 import BeautifulSoup
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
from Processor.App.ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    headline_transform,
    iso_date_transform,
    keywords_transform,
    text_unification_transform,
    text_unifications_transform,
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_transform,
)
from Processor.App.processor_utils import PipeMetadata


class DenikV3Extractor(ArticleExtractor):
    SINCE = datetime(2018, 12, 1)

    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "keywords": "meta[name='keywords']",
                "brief": "meta[property='og:description']",
                "author": "meta[name='author']",
                "publication_date": "meta[property='article:published_time']",
            },
            {
                "keywords": [
                    get_attribute_transform("content"),
                    lambda x: x.split(" "),
                    lambda x: ",".join(x),
                    keywords_transform,
                ],
                "headline": [get_attribute_transform("content"), headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
                "author": [get_attribute_transform("content"), author_transform],
                "publication_date": [
                    get_attribute_transform("content"),
                    iso_date_transform,
                ],
            },
            {
                "content": ".article-text",
                "category": "ul[class*=menu] > li > a.active",
                "keywords": "ul.article-tags-list",
                "author": ".section-article-autor",
            },
            {
                "content": [
                    lambda x: [
                        get_tags_transform(".article-text > p")(x),
                        get_tags_transform("div > div[class*=paywall]")(x),
                    ],
                    lambda x: list(map(get_text_transform, x[0]))
                    + list(map(article_content_transform(), x[1])),
                    lambda x: text_unification_transform(
                        "\n".join(text_unifications_transform(x))
                    ),
                ],
                "keywords": [get_text_transform, keywords_transform],
                "category": [get_text_transform, category_transform],
                "author": [get_text_transform, author_transform],
            },
            ".page",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict = {
            "comments_num": None,
        }
        return extracted_dict


extractor = DenikV3Extractor()
