import argparse
from datetime import datetime
import logging
from pathlib import Path
import re
from typing import List

import bs4
import asyncio
from OutStreamer.stream_to_file import (
    OutStreamerFileJSON,
)

from Router.router import Router
from utils import DomainRecord, PipeMetadata

FOLDER = "articles_processed"

logging.basicConfig(level=logging.INFO)


def parse_article(article_path: Path):
    with open(article_path, "r") as f:
        article = f.read()

    bs4_article = bs4.BeautifulSoup(article, "html.parser")
    url = None
    url_match = bs4_article.select_one("meta[property='og:url']")
    # This is terrible but works
    if url is None:
        if url_match:
            url = url_match.get("content")
    if url is None:
        url_match = bs4_article.select_one("link[rel='home']")
        if url_match:
            url = url_match.get("href")
            url += "/category/"
    if url is None:
        url_match = bs4_article.select_one("link[title='RSS']")
        if url_match:
            url = url_match.get("href")
            url += "/category/"

    year = re.search(r"\d{4}", article_path.name)
    if year is None:
        year = 2020
    else:
        year = int(year.group(0))
    logging.info("Found url: %s", url)

    metadata = PipeMetadata(
        DomainRecord(
            filename=article_path.stem,
            url=url,
            offset=0,
            length=100,
            timestamp=datetime(year, 1, 1),
        ),
        encoding="utf-8",
    )
    return article, metadata


async def article_process(article_path: List[Path], output_path: Path, router: Router):
    outstreamer = OutStreamerFileJSON(origin=output_path, pretty=True, order_num=False)
    for article in article_path:
        article, metadata = parse_article(article)

        extractor = router.route(metadata.domain_record.url)
        output = extractor.extract(article, metadata)
        if output is None:
            continue
        metadata.name = metadata.domain_record.filename
        path = await outstreamer.stream(output, metadata)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("article_path", nargs="+", type=Path)
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()

    # Router
    router = Router()
    router.load_modules(str(Path("DoneExtractors").absolute()))
    # router.register_route("aktualne_cz", [r".*aktualne\.cz.*"])
    # router.register_route("idnes_cz", [r".*idnes\.cz.*"])
    # router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
    router.register_route("irozhlas_cz", [r".*irozhlas\.cz.*"])
    # router.register_route("novinky_cz", [r".*novinky\.cz.*"])
    asyncio.run(article_process(**vars(args), router=router))
