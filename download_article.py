import logging
import sys
from pathlib import Path

sys.path.append(Path("Processor").absolute().as_posix())
sys.path.append(Path("Aggregator").absolute().as_posix())


import argparse
from datetime import datetime
from os import path
from pathlib import Path
from typing import List
from Processor.Downloader.download import Downloader
from Aggregator.index_query import DomainRecord, IndexAggregator
import asyncio
from Processor.OutStreamer.stream_to_file import (
    OutStreamerFileJSON,
    OutStreamerFileHTMLContent,
)
from Processor.Pipeline.pipeline import ProcessorPipeline

from Processor.Router.router import Router

FOLDER = "articles_downloaded"

logging.basicConfig(level="INFO")


async def article_download(
    url: str,
    cc_server: List[str] = [],
    since: datetime = datetime.min,
    to: datetime = datetime.max,
    limit: int = 5,
):
    # Sync queue because I didn't want to put effort in
    records: List[DomainRecord] = []

    # At start so we can fail faster
    router = Router()
    router.load_modules(str(Path("Processor/UserDefinedExtractors").absolute()))
    router.register_route("DummyExtractor", [r".*"])
    outstreamer = OutStreamerFileHTMLContent(origin=Path("./articles_downloaded_html"))

    aggregator = await IndexAggregator(
        [url], cc_servers=cc_server, since=since, to=to, limit=limit
    ).aopen()
    async for domain_record in aggregator:
        records.append(domain_record)
    await aggregator.aclose(None, None, None)

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
    parser.add_argument("--since", type=str, default=datetime.min)
    parser.add_argument("--to", type=str, default=datetime.max)
    args = vars(parser.parse_args())
    if isinstance(args["since"], str):
        args["since"] = datetime.fromisoformat(args["since"])

    if isinstance(args["to"], str):
        args["to"] = datetime.fromisoformat(args["to"])

    asyncio.run(article_download(**args))
