import asyncio
import logging
from pathlib import Path

import argparse
from datetime import datetime
from pathlib import Path
from typing import List
from Processor.App.Downloader.downloader import DownloaderFull
from Processor.App.Pipeline.pipeline import ProcessorPipeline
from Processor.App.processor_utils import (
    all_purpose_logger,
    metadata_logger,
    DomainRecord as DomainRecordProc,
)
from Processor.App.Router.router import Router
from Processor.App.OutStreamer.stream_to_file import OutStreamerFileHTMLContent
from Aggregator.App.index_query import IndexAggregator, DomainRecord as DomainRecordAgg


all_purpose_logger.setLevel(logging.INFO)
metadata_logger.setLevel(logging.WARN)


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
    records: List[DomainRecordAgg] = []

    # At start so we can fail faster
    router = Router()
    router.load_module(
        Path(__file__).parent / "Processor" / "App" / "Extractor" / "dummy_extractor.py"
    )
    router.register_route("dummy_extractor", [r".*"])
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

    all_purpose_logger.info(f"Downloaded {len(records)} articles")
    async with DownloaderFull(digest_verification=True) as downloader:
        try:
            pipeline = ProcessorPipeline(
                router=router, downloader=downloader, outstreamer=outstreamer
            )
            for dr in records:
                dr.encoding = dr.encoding if dr.encoding is not None else encoding
                dr_proc = DomainRecordProc(**dr.__dict__)
                await pipeline.process_domain_record(dr_proc)
        except Exception as e:
            all_purpose_logger.error(e, exc_info=True)
            raise e
    all_purpose_logger.info("Finished pipeline")


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
