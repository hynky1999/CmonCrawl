from datetime import datetime
import json
import logging
import os
from pathlib import Path

from Processor.App.Downloader.dummy_downloader import DownloaderDummy
from Processor.App.Pipeline.pipeline import ProcessorPipeline


import argparse
from typing import List
import asyncio
from Processor.App.OutStreamer.stream_to_file import (
    OutStreamerFileJSON,
)

from Processor.App.Router.router import Router
from Processor.App.processor_utils import (
    DomainRecord,
    all_purpose_logger,
    metadata_logger,
)

all_purpose_logger.setLevel(logging.DEBUG)
metadata_logger.setLevel(logging.DEBUG)


async def article_process(
    article_path: List[Path],
    output_path: Path,
    config_path: Path,
    extractors_path: Path,
    url: str | None,
    date: datetime | None,
):

    with open(config_path, "r") as f:
        config = json.load(f)
    router = Router()
    router.load_modules(extractors_path)
    router.register_routes(config.get("routes", []))
    downloader = DownloaderDummy(article_path, url, date)
    outstreamer = OutStreamerFileJSON(origin=output_path, pretty=True, order_num=False)
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    # Will be changed anyway
    dummy_record = DomainRecord("", "", 0, 0)
    for path in article_path:
        created_paths = await pipeline.process_domain_record(dummy_record)
        if len(created_paths) == 0:
            continue
        created_path = created_paths[0]
        os.rename(created_path, created_path.parent / (path.stem + ".json"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("article_path", nargs="+", type=Path)
    parser.add_argument("output_path", type=Path)
    parser.add_argument(
        "--config_path",
        type=Path,
        default=Path(__file__).parent / "App" / "config.json",
    )
    parser.add_argument(
        "--extractors_path",
        type=Path,
        default=Path(Path(__file__).parent / "App" / "DoneExtractors"),
    )
    parser.add_argument("--date", type=str)
    parser.add_argument("--url", type=str)
    args = vars(parser.parse_args())
    if isinstance(args["date"], str):
        args["date"] = datetime.fromisoformat(args["date"])

    asyncio.run(article_process(**args))
