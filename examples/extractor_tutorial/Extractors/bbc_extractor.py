from cmoncrawl.processor.pipeline.extractor import PageExtractor
from cmoncrawl.processor.extraction.utils import check_required, get_text_transform


class BBCExtractor(PageExtractor):
    def __init__(self):
        super().__init__(
            header_css_dict={},
            header_extract_dict={},
            content_css_dict={
                "title": "h1#content",
                "content": "main[role=main]",
            },
            # Here we define how to transform the content of the tag into a string.
            content_extract_dict={
                "title": [get_text_transform],
                "content": [
                    get_text_transform,
                ],
            },
            # Here we define how to bind a tag that containt all fields we will use in article_css_dict
            # If you don't know just use body
            content_css_selector="body",
            # We required both title and content to be extracted and non empty
            is_valid_extraction=check_required(
                {"title": True, "content": True},
                "BBCExtractor",
                non_empty=True,
            ),
        )


page_extractor = BBCExtractor()
