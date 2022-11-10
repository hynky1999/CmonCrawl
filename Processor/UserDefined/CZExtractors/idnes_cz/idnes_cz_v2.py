from datetime import datetime
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
from Processor.App.ArticleUtils.article_utils import (
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    comments_num_transform,
    cz_date_transform,
    headline_transform,
    iso_date_transform,
    keywords_transform,
    date_transform
)
from Processor.App.Extractor.extractor_utils import (
    get_attribute_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)


class IdnesCZV2Extractor(ArticleExtractor):
    ENCODING = "windows-1250"
    SINCE = datetime(2011, 8, 9)

    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "keywords": "meta[name='keywords']",
                "publication_date": "meta[property='article:published_time']",
                "brief": "meta[property='og:description']",
            },
            {
                "keywords": [get_attribute_transform("content"), keywords_transform],
                "publication_date": [
                    get_attribute_transform("content"),
                    iso_date_transform,
                ],
                "headline": [get_attribute_transform("content"), headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
            },
            {
                "content": "#art-text > div.bbtext",
                "author": "div.authors",
                "comments_num": "#moot-linkin",
                "publication_date": "div.art-info > span.time",
                "category": "li.act > a",
            },
            {
                "content": article_content_transform(),
                "brief": [get_text_transform, brief_transform],
                "author": [
                    get_tags_transform("div span[itemprop='name']"),
                    get_text_list_transform(","),
                    author_transform,
                ],
                "comments_num": [get_text_transform, comments_num_transform],
                "publication_date": [
                    get_text_transform,
                    cz_date_transform,
                    date_transform,
                ],
                "category": [get_text_transform, category_transform],
            },
            "#main",
            filter_must_not_exist=[
                # Prevents Premium "articles"
                "div#paywall-unlock",
                "div#paywall",
            ],
        )


extractor = IdnesCZV2Extractor()
