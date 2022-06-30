import argparse
from datetime import datetime
from os import path
from pathlib import Path
from typing import List
from Aggregator.constants import MAX_DATE, MIN_DATE
from Processor.Downloader.download import Downloader
from Aggregator.index_query import DomainRecord, IndexAggregator
import asyncio
from Processor.OutStreamer.stream_to_file import (
    OutStreamerFileDefault,
    OutStreamerFileJSON,
)
from Processor.Pipeline.pipeline import ProcessorPipeline

from Processor.Router.router import Router

FOLDER = "articles_downloaded"


async def article_download(
    url: str,
    cc_server: List[str] = [],
    since: datetime = MIN_DATE,
    to: datetime = MAX_DATE,
    limit: int = 5,
):
    # Sync queue because I didn't want to put effort in
    records: List[DomainRecord] = []

    # At start so we can fail faster
    router = Router()
    router.load_modules(str(Path("UserDefinedExtractors").absolute()))
    router.register_route("aktualne_cz", [r".*aktualne\.cz.*"])
    router.register_route("idnes_cz", [r".*idnes\.cz.*"])
    router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
    outstreamer = OutStreamerFileJSON(origin=Path("./articles_downloaded"))

    async with IndexAggregator(
        [url], cc_servers=cc_server, since=since, to=to, limit=limit
    ) as aggregator:
        try:
            async for domain_record in aggregator:
                records.append(domain_record)
        except Exception as e:
            print(e)

    print(f"Downloaded {len(records)} articles")
    async with Downloader(digest_verification=True) as downloader:
        pipeline = ProcessorPipeline(
            router=router, downloader=downloader, outstreamer=outstreamer
        )
        for dr in records:
            try:
                await pipeline.process_domain_record(dr)
            except Exception as e:
                print(e)
    print("Finished pipeline")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("url")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--since", default=MIN_DATE)
    parser.add_argument("--to", default=MAX_DATE)
    args = parser.parse_args()
    asyncio.run(article_download(**vars(args)))
