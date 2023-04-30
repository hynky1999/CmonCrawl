from functools import reduce
from itertools import accumulate
from typing import Any, Callable, Dict, List
from bs4 import Tag

"""
 Whole point of these functions is that is possible to use them
 without lamdas which looks ugly imo. The reason for using transform
 is that we don't have to check for None values like crazy aka once
 None always None.

 The proble is that it doesn't have any sort of static checking
 which really sucks and debuging is a bit harder.

 No idea how to enforce this since the output can be anything.
"""


def get_tag_transform(tag_desc: str):
    def transform(tag: Tag):
        return tag.select_one(tag_desc)

    return transform


def get_tags_transform(tag_desc: str):
    def transform(tag: Tag):
        return tag.select(tag_desc)

    return transform


def get_attribute_transform(attr_name: str):
    def transform(tag: Tag):
        return tag.get(attr_name, None)

    return transform


def get_text_transform(tag: Tag, recursive: bool = True):
    if recursive:
        return tag.text
    tag_text = tag.find(text=True, recursive=False)
    if tag_text:
        return tag_text.text

    return None


def get_text_list_transform(sep: str = ""):
    def transform(tag: List[Tag]):
        return sep.join([tag.text for tag in tag])

    return transform


def all_same_transform(
    dict: Dict[str, Any], fc: Callable[[Any], Any] | List[Callable[[Any], Any]]
):
    return {key: fc for key in dict.keys()}


def chain_transforms(trans: List[Callable[[Any], Any]]):
    def inner(initial_value: Any):
        result = initial_value
        for fc in trans:
            if result is None:
                break
            result = fc(result)
        return result

    return inner


def transform(
    dict: Dict[str, Any],
    transforms: Dict[str, Callable[[Any], Any] | List[Callable[[Any], Any]]],
):
    def transform_fc(key: str, value: Any):
        key_trans = transforms.get(key, [])
        if not isinstance(key_trans, list):
            key_trans = [key_trans]
        for trans in key_trans:
            if value is None:
                break
            value = trans(value)
        return value

    return {key: transform_fc(key, value) for key, value in dict.items()}


def extract_transform(
    tag: Tag | None,
    extract_dict: Dict[str, str],
    extract_transform_dict: Dict[
        str, Callable[[Any], Any] | List[Callable[[Any], Any]]
    ],
) -> Dict[str, Any]:
    if tag is None:
        return dict()

    extracted_tags = transform(
        extract_dict, all_same_transform(extract_dict, lambda x: tag.select_one(x))
    )
    extracted_data = transform(extracted_tags, extract_transform_dict)

    return extracted_data


def combine_dicts(dicts: List[Dict[str, Any]]):
    # Combines dicts choose the first one that is not None.
    def recursive_get(key: str, dicts: List[Dict[str, Any]], i: int) -> Any:
        if i >= len(dicts):
            return None
        val = dicts[i].get(key, None)
        if val is None:
            return recursive_get(key, dicts, i + 1)
        return val

    keys = [key for d in dicts for key in d.keys()]
    return {key: recursive_get(key, dicts, 0) for key in keys}
