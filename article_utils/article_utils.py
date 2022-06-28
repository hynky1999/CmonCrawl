from typing import Any, Callable, Dict

from bs4 import BeautifulSoup, NavigableString, Tag

from Processor.Extractor.extractor_utils import (
    TagDescriptor,
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


def article_transform(article: Tag):
    if article is None:
        return None

    ps = article.find_all(["p", *[f"h{i}" for i in range(1, 7)]], recursive=False)
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
    head_extract_dict: Dict[str, TagDescriptor],
    head_extract_transform_dict: Dict[str, Callable[[str], Any]],
) -> Dict[str, Any]:
    head = soup.find("head")
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
    article_extract_dict: Dict[str, TagDescriptor],
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
    # Not name
    if len(text.split(" ")) < 2:
        return None

    return text


def must_exist_filter(soup: BeautifulSoup, filter_dict: Dict[str, Any]):
    must_exist = [
        soup.find(tag_desc.tag, **tag_desc.attrs) for tag_desc in filter_dict.values()
    ]
    if any(map(lambda x: x is None, must_exist)):
        return False

    return True


def must_not_exist_filter(soup: BeautifulSoup, filter_dict: Dict[str, Any]):
    must_not_exist = [
        soup.find(tag_desc.tag, **tag_desc.attrs) for tag_desc in filter_dict.values()
    ]
    if any(map(lambda x: x is not None, must_not_exist)):
        return False

    return True
