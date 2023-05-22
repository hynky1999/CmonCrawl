from datetime import datetime
from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
from Processor.App.ArticleUtils.article_utils import (
    headline_transform,
    get_text_transform,
    text_unifications_transform,
)

REQUIRED_FIELDS = {"title": False, "content": True}


def content_transform(soup):
    return [p.text for p in soup.find_all("p", recursive=True)]


class BBCExtractor(ArticleExtractor):
    SINCE = datetime(2021, 1, 20)
    TO = datetime(2021, 3, 20)

    def __init__(self):
        super().__init__(
            header_css_dict={},
            header_extract_dict={},
            article_css_dict={
                "title": "h1#content",
                "content": "main[role=main]",
            },
            # Here we define how to transform the content of the tag into a string.
            article_extract_dict={
                "title": [get_text_transform, headline_transform],
                "content": [
                    content_transform,
                    text_unifications_transform,
                    lambda lines: "\n".join(lines),
                ],
            },
            # Here we define how to bind a tag that containt all fields we will use in article_css_dict
            # If you don't know just use body
            article_css_selector="body",
            required_fields=REQUIRED_FIELDS,
            non_empty=True,
        )


extractor = BBCExtractor()
