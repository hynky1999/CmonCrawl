import sys
from pathlib import Path

sys.path.append(Path("Processor/App").absolute().as_posix())
sys.path.append(Path("Aggregator/App").absolute().as_posix())


import argparse
from datetime import datetime
from pathlib import Path
from typing import List
from Downloader.download import Downloader
from index_query import DomainRecord, IndexAggregator
import asyncio
from OutStreamer.stream_to_file import (
    OutStreamerFileHTMLContent,
)
from Pipeline.pipeline import ProcessorPipeline

from Router.router import Router
from processor_utils import all_purpose_logger, metadata_logger

all_purpose_logger.setLevel("INFO")
metadata_logger.setLevel("WARN")


async def article_download(
    url: str,
    output: Path,
    cc_server: List[str] = [],
    since: datetime = datetime.min,
    to: datetime = datetime.max,
    limit: int = 5,
    encoding: str = "utf-8",
):
    # Sync queue because I didn't want to put effort in
    records: List[DomainRecord] = []

    # At start so we can fail faster
    router = Router()
    router.load_module(Path("Processor/App/Extractor/DummyExtractor.py").absolute())
    router.register_route("DummyExtractor", [r".*"])
    outstreamer = OutStreamerFileHTMLContent(origin=output)

    aggregator = await IndexAggregator(
        [url],
        cc_servers=cc_server,
        since=since,
        to=to,
        limit=limit,
        max_retry=15,
        sleep_step=10,
        prefetch_size=1,
    ).aopen()
    async for domain_record in aggregator:
        records.append(domain_record)
    await aggregator.aclose(None, None, None)

    print(f"Downloaded {len(records)} articles")
    downloader = await Downloader(digest_verification=True).aopen()
    pipeline = ProcessorPipeline(
        router=router, downloader=downloader, outstreamer=outstreamer
    )
    for dr in records:
        dr.encoding = dr.encoding if dr.encoding is not None else encoding
        await pipeline.process_domain_record(dr)
    await downloader.aclose(None, None, None)
    print("Finished pipeline")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("url")
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--since", type=str, default=datetime.min)
    parser.add_argument("--to", type=str, default=datetime.max)
    parser.add_argument("--encoding", type=str, default="utf-8")
    args = vars(parser.parse_args())
    if isinstance(args["since"], str):
        args["since"] = datetime.fromisoformat(args["since"])

    if isinstance(args["to"], str):
        args["to"] = datetime.fromisoformat(args["to"])

    asyncio.run(article_download(**args))
