import json
import sys
from pathlib import Path

sys.path.append(Path("App").absolute().as_posix())


import argparse
from datetime import datetime
import re
from typing import Any, Dict, List

import bs4
import asyncio
from OutStreamer.stream_to_file import (
    OutStreamerFileJSON,
)

from Router.router import Router
from processor_utils import (
    DomainRecord,
    PipeMetadata,
    all_purpose_logger,
    metadata_logger,
)

# sys.path.append(Path("Processor").absolute().as_posix())
# sys.path.append(Path("Aggregator").absolute().as_posix())

FOLDER = "articles_processed"

all_purpose_logger.setLevel("DEBUG")
metadata_logger.setLevel("DEBUG")


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
        url_match = bs4_article.select_one("link[title*='RSS']")
        if url_match:
            url = url_match.get("href")
            url += "/category/"
    if url is None:
        url_match = bs4_article.select_one("link[media*='handheld']")
        if url_match:
            url = url_match.get("href")
            url += "/category/"

    year = re.search(r"\d{4}", article_path.name)
    if year is None:
        year = 2020
    else:
        year = int(year.group(0))
    all_purpose_logger.debug(f"Found url: {url}")

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


async def article_process(
    article_path: List[Path], output_path: Path, config_path: Path
):

    with open(config_path, "r") as f:
        config = json.load(f)
    router = Router()
    router.load_modules(str(Path("App/DoneExtractors").absolute()))
    router.register_routes(config.get("routes", []))
    outstreamer = OutStreamerFileJSON(origin=output_path, pretty=True, order_num=False)
    for article in article_path:
        article, metadata = parse_article(article)

        try:
            extractor = router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            output = extractor.extract(article, metadata)
            if output is None:
                continue
            metadata.name = metadata.domain_record.filename
            await outstreamer.stream(output, metadata)
        except Exception as e:
            metadata_logger.error(
                e, exc_info=True, extra={"domain_record": metadata.domain_record}
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("article_path", nargs="+", type=Path)
    parser.add_argument("output_path", type=Path)
    parser.add_argument("--config_path", type=Path, default=Path("App/config.json"))
    args = parser.parse_args()

    asyncio.run(article_process(**vars(args)))
