from typing import Any, Callable, Dict
from bs4 import NavigableString, Tag


def get_tag_transform(tag: Tag | NavigableString | None):
    def transform(tag_desc: str):
        if tag is None or isinstance(tag, NavigableString):
            return None

        tag_found = tag.select_one(tag_desc)
        if tag_found is None:
            return None
        return tag_found

    return transform


def get_attribute_transform(attr_name: str):
    def transform(tag: Tag | NavigableString | None):
        if tag is None or isinstance(tag, NavigableString):
            return None

        meta_content = tag.get(attr_name, None)
        if meta_content is None:
            return None
        meta_content_str: str = (
            meta_content if isinstance(meta_content, str) else " ".join(meta_content)
        )

        return meta_content_str

    return transform


def all_same_transform(
    dict: Dict[str, Any], fc: Callable[[Any], Any | None]
) -> Dict[str, Callable[[object], object]]:
    return {key: fc for key in dict.keys()}


def transform(dict: Dict[str, Any], transforms: Dict[str, Callable[[Any], Any | None]]):
    return {
        key: transforms.get(key, lambda x: x)(value) if value is not None else None
        for key, value in dict.items()
    }
