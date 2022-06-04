from random import betavariate
from typing import Any, Dict, Union
from bs4 import BeautifulSoup, BeautifulStoneSoup
from Processor.Extractor.extractor import BaseExtractor


class AktualneExtractor(BaseExtractor):
    def extract_no_preprocess(
        self, response: str, pipe_params: Dict[str, Any]
    ) -> Union[None, str]:
        extracted = {}
       soup = BeautifulSoup(response, "html.parser")
        article_soup = soup.find("div", class_="article")
        if article_soup is None:
            return None
        extracted["headline"] = article_soup.find(
            "div", class_="article__perex"
        ).get_text("")
        extracted["article"] = article_soup.find("div", id="article-content").get_text(
            " "
        )
        extracted["author"] = article_soup.find("div", class_="author__name").get_text(
            " "
        )

