from Processor.App.ArticleUtils.article_utils import (
    ALLOWED_H,
    LIST_TAGS,
    TABLE_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
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
from Processor.App.processor_utils import PipeMetadata
from bs4 import BeautifulSoup
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor


class IrozhlasExtractor(ArticleExtractor):
    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
            },
            {
                "headline": [get_attribute_transform("content"), headline_transform],
            },
            {
                "content": "div.b-detail",
                "author": "div.b-detail > p.meta strong",
                "publication_date": "header.b-detail__head > p.meta time",
                "keywords": "nav.m-breadcrumb",
                "category": "nav.m-breadcrumb > a:nth-child(3)",
                "brief": ".b-detail > header > p ",
            },
            {
                "content": article_content_transform(
                    fc_eval=(
                        lambda x: (x.name in [*ALLOWED_H, "p"] and len(x.attrs) == 0)
                        or (
                            # image
                            x.name
                            in [*LIST_TAGS, "figure", *TABLE_TAGS]
                        )
                    )
                ),
                "author": [get_text_transform, author_transform],
                "publication_date": [
                    get_text_transform,
                    cz_date_transform,
                    format_date_transform("%H:%M %d. %m. %Y"),
                ],
                "keywords": [
                    get_tags_transform(
                        "nav > a[href*='zpravy-tag']",
                    ),
                    get_text_list_transform(","),
                    keywords_transform,
                ],
                "category": [get_text_transform, brief_transform],
                "brief": [get_text_transform, brief_transform],
            },
            "main#main",
            filter_must_exist=[  # Prevents Premium "articles"
                "#menu-main",
            ],
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        return {"comments_num": None}


extractor = IrozhlasExtractor()
