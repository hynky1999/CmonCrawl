# from typing import Any, Callable, Dict
# from utils import PipeMetadata
# from bs4 import BeautifulSoup, Tag
# from ArticleUtils.article_extractor import ArticleExtractor

# from ArticleUtils.article_utils import (
#     ALLOWED_H,
#     article_extract_transform,
#     article_transform,
#     author_extract_transform,
#     extract_publication_date,
#     head_extract_transform,
# )


# head_extract_dict: Dict[str, str] = {
#     "headline": "meta[property='og:title']",
#     "keywords": "meta[name='keywords']",
# }


# head_extract_transform_dict: Dict[str, Callable[[str], Any]] = {
#     "keywords": lambda x: x.split(","),
#     "headline": lambda x: x.replace(r" - iDNES.cz", "").strip(),
# }


# article_extract_dict: Dict[str, Any] = {
#     "brief": "p[data-dot='ogm-article-perex']",
#     "content": "div[class*='mol-rich-content--for-article']",
#     "author": "div[data-dot='ogm-article-author'] span[data-dot='mol-author-names']",
#     "publication_date": "div[data-dot='ogm-date-of-publication__date']",
# }

# article_extract_transform_dict: Dict[str, Callable[[Tag], Any]] = {
#     "content": lambda x: article_transform(
#         x,
#         fc_eval=lambda x: x.name in ALLOWED_H
#         or (x.name == "div" and x.attrs.get("data-dot", "") == "mol-paragraph"),
#     ),
#     "brief": lambda x: x.text if x else None,
#     "author": author_extract_transform,
#     "publication_date": extract_publication_date("%d. %m. %Y %H:%M"),
# }


# filter_head_extract_dict: Dict[str, Any] = {
#     "type": "meta[property='og:type']",
# }


# class Extractor(ArticleExtractor):
#     def article_extract(self, soup: BeautifulSoup, metadata: PipeMetadata):
#         extracted_head = head_extract_transform(
#             soup, head_extract_dict, head_extract_transform_dict
#         )

#         extracted_article = article_extract_transform(
#             soup.select_one("section[data-dot='tpl-content']"),
#             article_extract_dict,
#             article_extract_transform_dict,
#         )

#         # merge dicts
#         extracted_dict = {**extracted_head, **extracted_article}
#         extracted_dict["category"] = None
#         extracted_dict["comments_num"] = None

#         return extracted_dict

#     def filter(self, response: str, metadata: PipeMetadata):
#         soup = BeautifulSoup(response, "html.parser")

#         return True
