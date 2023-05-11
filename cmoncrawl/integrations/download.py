from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, List
from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.processor.pipeline.downloader import AsyncDownloader
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.processor.pipeline.streamer import StreamerFileHTML
from cmoncrawl.processor.pipeline.extractor import HTMLExtractor, DomainRecordExtractor
from cmoncrawl.middleware.synchronized import index_and_extract
import argparse
import asyncio
from cmoncrawl.processor.pipeline.streamer import (
    StreamerFileJSON,
)

from cmoncrawl.processor.pipeline.router import Router


class DownloadOutputFormat(Enum):
    RECORD = "record"
    HTML = "html"


def add_mode_args(subparser: Any):
    record_parser = subparser.add_parser(DownloadOutputFormat.RECORD.value)
    record_parser.add_argument("--max_crawls_per_file", type=int, default=500_000)
    subparser.add_parser(DownloadOutputFormat.HTML.value)
    return subparser


def add_args(subparser: Any):
    parser = subparser.add_parser("download")
    parser.add_argument("url")
    mode_subparser = parser.add_subparsers(dest="mode", required=True)
    mode_subparser = add_mode_args(mode_subparser)
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--since", type=datetime.fromisoformat, default=str(datetime.min)
    )
    parser.add_argument("--to", type=datetime.fromisoformat, default=str(datetime.max))
    parser.add_argument("--cc_server", nargs="+", type=str, default=None)
    parser.add_argument("--max_retry", type=int, default=30)
    parser.add_argument("--sleep_step", type=int, default=4)
    # Add option to output to either json or html
    parser.add_argument("--max_directory_size", type=int, default=1000)
    parser.add_argument("--filter_non_200", action="store_true", default=True)
    parser.set_defaults(func=run_download)


def url_download_prepare_router(
    output_format: DownloadOutputFormat, filter_non_200: bool
):
    router = Router()
    match output_format:
        case DownloadOutputFormat.RECORD:
            router.load_extractor(
                "dummy_extractor", DomainRecordExtractor(filter_non_ok=filter_non_200)
            )
        case DownloadOutputFormat.HTML:
            router.load_extractor(
                "dummy_extractor", HTMLExtractor(filter_non_ok=filter_non_200)
            )
    router.register_route("dummy_extractor", [r".*"])
    return router


def url_download_prepare_streamer(
    output_format: DownloadOutputFormat,
    output: Path,
    max_direrctory_size: int,
    max_crawls_per_file: int,
):
    match output_format:
        case DownloadOutputFormat.RECORD:
            return StreamerFileJSON(
                root=output,
                max_directory_size=max_direrctory_size,
                max_file_size=max_crawls_per_file,
            )
        case DownloadOutputFormat.HTML:
            return StreamerFileHTML(root=output, max_directory_size=max_direrctory_size)


async def url_download(
    url: str,
    output: Path,
    cc_server: List[str] | None,
    since: datetime,
    to: datetime,
    limit: int,
    max_retry: int,
    sleep_step: int,
    mode: DownloadOutputFormat,
    max_crawls_per_file: int,
    max_directory_size: int,
    filter_non_200: bool,
):
    outstreamer = url_download_prepare_streamer(
        mode, output, max_directory_size, max_crawls_per_file
    )
    router = url_download_prepare_router(mode, filter_non_200)
    pipeline = ProcessorPipeline(
        router, AsyncDownloader(max_retry=max_retry), outstreamer
    )

    index_agg = IndexAggregator(
        cc_servers=cc_server or [],
        domains=[url],
        since=since,
        to=to,
        limit=limit,
        max_retry=max_retry,
        sleep_step=sleep_step,
    )
    await index_and_extract(index_agg, pipeline)


def run_download(args: argparse.Namespace):
    mode = DownloadOutputFormat(args.mode)
    return asyncio.run(
        url_download(
            args.url,
            args.output,
            args.cc_server,
            args.since,
            args.to,
            args.limit,
            args.max_retry,
            args.sleep_step,
            mode,
            args.max_crawls_per_file if mode == DownloadOutputFormat.RECORD else 1,
            args.max_directory_size,
            args.filter_non_200,
        )
    )
