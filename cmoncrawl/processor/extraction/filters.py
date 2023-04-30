from typing import List
from bs4 import BeautifulSoup


def must_exist_filter(soup: BeautifulSoup, filter_list: List[str]):
    """
    This function takes in a BeautifulSoup object and a list of
    CSS selectors.
    If all selectors are found in the soup, this function returns True.
    """
    must_exist = [soup.select_one(css_selector) for css_selector in filter_list]
    if any(map(lambda x: x is None, must_exist)):
        return False

    return True


def must_not_exist_filter(soup: BeautifulSoup, filter_list: List[str]):
    """
    This function takes in a BeautifulSoup object and a list of
    CSS selectors.
    If any selector is found in the soup, this function returns False.
    """
    must_not_exist = [soup.select_one(css_selector) for css_selector in filter_list]
    if any(map(lambda x: x is not None, must_not_exist)):
        return False

    return True
