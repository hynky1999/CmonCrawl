from dataclasses import dataclass
from typing import Any, Callable, Dict
from bs4 import Tag


@dataclass
class TagDescriptor:
    tag: str = ""
    attrs: Dict[str, str] = dict()


def get_tag_transform(tag: Tag):
    def transform(tag_desc: TagDescriptor):
        tag_found = tag.find(tag_desc.tag, attrs=tag_desc.attrs)
        if tag_found is None:
            return None
        return tag_found

    return transform


def get_attribute_transform(attr_name: str):
    def transform(tag: Tag):
        meta_content = tag.get(attr_name, None)
        meta_content_str: str = (
            meta_content if meta_content is str else " ".join(meta_content)
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
