from typing import Any, Callable, Dict, List, Sized
from bs4 import Tag

from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.common.loggers import metadata_logger

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
    """
    Returns a function that takes a bs4 tag and returns the first tag
    that matches the tag_desc.

    Args:
        tag_desc (str): CSS selector

    """

    def transform(tag: Tag):
        return tag.select_one(tag_desc)

    return transform


def get_tags_transform(tag_desc: str):
    """
    Returns a function that takes a bs4 tag and returns a list of tags
    that match the tag_desc.

    Args:
        tag_desc (str): CSS selector

    """

    def transform(tag: Tag):
        return tag.select(tag_desc)

    return transform


def get_attribute_transform(attr_name: str):
    """
    Returns a function that takes a bs4 tag and returns the value
    of the attribute `attr_name` or None if the attribute doesn't exist.

    Args:
        attr_name (str): Name of the attribute to get from the tag
    """

    def transform(tag: Tag):
        return tag.get(attr_name, None)

    return transform


def get_text_transform(tag: Tag, recursive: bool = True):
    """
    Returns text from tag. If recursive is True then
    all text from all children is returned.

    Args:
        tag (Tag): bs4 tag
        recursive (bool, optional): If True then all text from all children is returned. Defaults to True.


    """

    if recursive:
        return tag.text
    tag_text = tag.find(text=True, recursive=False)
    if tag_text:
        return tag_text.text

    return None


def get_text_list_transform(sep: str = ""):
    """
    Returns a function that takes a list of bs4 tags and returns
    a string with all the text from the tags joined with `sep`.

    Args:
        sep (str, optional): Separator to use when joining the text. Defaults to "".
    """

    def transform(tag: List[Tag]):
        return sep.join([tag.text for tag in tag])

    return transform


def all_same_transform(
    dict: Dict[str, Any], fc: Callable[[Any], Any] | List[Callable[[Any], Any]]
):
    """
    Applies `fc` to all values in `dict` and returns a dict with same keys
    but with transformed values.

    Args:
        dict (Dict[str, Any]): Dict to transform.
        fc (Callable[[Any], Any] | List[Callable[[Any], Any]]): Function to apply to all values in dict.


    """
    return {key: fc for key in dict.keys()}


def chain_transforms(trans: List[Callable[[Any], Any]]):
    """
    Chains transforms together. If any of the transforms returns None
    the chain is broken and None is returned.

    Args:
        trans (List[Callable[[Any], Any]]): List of transforms to chain together.


    """

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
    """
    Transforms dict using `transforms` dict. `transforms` dict is of format
    `{key: [transform1, transform2, ...]}` where transform is a function that takes previous value

    Args:
        dict (Dict[str, Any]): Dict to transform.
        transforms (Dict[str, Callable[[Any], Any] | List[Callable[[Any], Any]]]): Dict defining
            how to transform the dict. Format is "{name: [transform1, transform2, ...]}" where
            transform is a function that takes previous value and returns new value.
    """

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
    """
    Extracts data from tag using `extract_dict` defining what to extract and how to name it,
    and `extract_transform_dict` defining how to transform the extracted data.

    Args:
        tag (Tag | None): Tag to extract data from.
        extract_dict (Dict[str, str]): Dict defining what to extract and how to name it. format
            is `{"name": "css selector"}`.
        extract_transform_dict (Dict[str, Callable[[Any], Any] | List[Callable[[Any], Any]]]): Dict
            defining how to transform the extracted data. Format is "{name: [transform1, transform2, ...]}"
            where transform is a function that takes previous value and returns new value.
    """

    if tag is None:
        return dict()

    extracted_tags = transform(
        extract_dict, all_same_transform(extract_dict, lambda x: tag.select_one(x))
    )
    extracted_data = transform(extracted_tags, extract_transform_dict)

    return extracted_data


def combine_dicts(dicts: List[Dict[str, Any]]):
    """
    Combines list of dictioneries into one. If there are multiple values for the same key
    then the first one that is not None is chosen.

    Args:
        dicts (List[Dict[str, Any]]): List of dicts to combine.
    """

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


def check_required(
    required_fields: Dict[str, bool], extractor_name: str, non_empty: bool = False
):
    """
    Checks if required fields are present in the extracted dict.

    Args:
        required_fields (Dict[str, bool]): Dict of required fields if defining which
            fields must be present and which can be None.

        extractor_name (str): Name of the extractor for logging purposes.

        non_empty (bool, optional): If True then empty strings and empty lists are considered
            as not present. Defaults to False.

    """

    def inner(extracted_dict: Dict[Any, Any], metadata: PipeMetadata):
        for key, value in required_fields.items():
            if key not in extracted_dict:
                metadata_logger.warn(
                    f"{extractor_name} failed to extract {key}",
                    extra={"domain_record": metadata.domain_record},
                )
                return False
            extracted_val = extracted_dict[key]
            if value:
                if extracted_val is None:
                    metadata_logger.warn(
                        f"{extractor_name}: None for key: {key} is not allowed",
                        extra={"domain_record": metadata.domain_record},
                    )
                    return False

                if non_empty:
                    if isinstance(extracted_val, str) and extracted_val == "":
                        metadata_logger.warn(
                            f"{extractor_name}: empty string for key: {key} is not allowed",
                            extra={"domain_record": metadata.domain_record},
                        )
                        return False
                    if isinstance(extracted_val, Sized) and len(extracted_val) == 0:
                        metadata_logger.warn(
                            f"{extractor_name}: empty list for key: {key} is not allowed",
                            extra={"domain_record": metadata.domain_record},
                        )
                        return False
        return True

    return inner
