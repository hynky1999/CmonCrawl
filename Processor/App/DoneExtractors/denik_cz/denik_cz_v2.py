from datetime import datetime

from bs4 import BeautifulSoup
from ArticleUtils.article_extractor import ArticleExtractor
from ArticleUtils.article_utils import (
    ALLOWED_H,
    LIST_TAGS,
    TABLE_TAGS,
    article_content_transform,
    author_transform,
    brief_transform,
    category_transform,
    comments_num_transform,
    date_complex_extract,
    headline_transform,
    keywords_transform,
)
from Extractor.extractor_utils import (
    get_attribute_transform,
    get_tag_transform,
    get_tags_transform,
    get_text_list_transform,
    get_text_transform,
)
from processor_utils import PipeMetadata


class DenikV2Extractor(ArticleExtractor):
    SINCE = datetime(2012, 2, 29)
    TO = datetime(2018, 12, 1)

    def __init__(self):
        super().__init__(
            {
                "headline": "meta[property='og:title']",
                "keywords": "meta[name='keywords']",
                "brief": "meta[property='og:description']",
            },
            {
                "keywords": [
                    get_attribute_transform("content"),
                    keywords_transform,
                ],
                "headline": [get_attribute_transform("content"), headline_transform],
                "brief": [get_attribute_transform("content"), brief_transform],
            },
            {
                "content": ".bbtext",
                "author": "p[class*=clanek-autor]",
                "category": "ul[class*=menu] > li > a.active",
            },
            {
                "content": article_content_transform(
                    fc_eval=lambda x: (
                        x.name in [*ALLOWED_H, "p"] and len(x.attrs) == 0
                    )
                    or (
                        # image
                        x.name
                        in [*LIST_TAGS, "figure", *TABLE_TAGS]
                    )
                ),
                "author": [
                    get_text_transform,
                    author_transform,
                ],
                "category": [get_text_transform, category_transform],
            },
            ".page-content",
        )

    def custom_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
        extracted_dict = {
            "publication_date": date_complex_extract(
                soup,
                [
                    ".page-content div[class*=clanek-datum] > .datum",
                    ".page-content  p.clanek-datum",
                ],
                ["%d.%m.%Y %H:%M", "%d.%m.%Y"],
                hours_minutes_with_fallback=True,
                fallback=metadata.domain_record.timestamp,
            ),
            "comments_num": self.custom_extract_comments_num(soup),
            "author": self.custom_extract_author(soup),
        }
        return extracted_dict

    def custom_extract_comments_num(self, soup: BeautifulSoup):
        comments_tag = get_tag_transform(".page-content .diskuse-vstup")(soup)
        if comments_tag:
            return comments_num_transform(get_text_transform(comments_tag))

        no_comments_tag = get_tag_transform("#frm-diskuse-diskuseForm")(soup)
        if no_comments_tag:
            return "0"

        return None

    def custom_extract_author(self, soup: BeautifulSoup):
        author_tag = get_tag_transform(
            ".page-content .bbtext > p[style='text-align: right;']"
        )(soup)
        if author_tag:
            return author_transform(get_text_transform(author_tag))

        return None


extractor = DenikV2Extractor()
