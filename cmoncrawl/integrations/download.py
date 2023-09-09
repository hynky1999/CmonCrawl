from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, List
from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.common.types import MatchType
from cmoncrawl.processor.pipeline.downloader import AsyncDownloader
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.processor.pipeline.streamer import StreamerFileHTML
from cmoncrawl.processor.pipeline.extractor import HTMLExtractor, DomainRecordExtractor
from cmoncrawl.middleware.synchronized import query_and_extract
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
    record_parser = subparser.add_parser(
        DownloadOutputFormat.RECORD.value,
        help="Download record files from Common Crawl",
    )
    record_parser.add_argument(
        "--max_crawls_per_file",
        type=int,
        default=500_000,
        help="Max number of domain records per file output",
    )
    html_parser = subparser.add_parser(
        DownloadOutputFormat.HTML.value, help="Download HTML files from Common Crawl"
    )

    html_parser.add_argument(
        "--encoding",
        type=str,
        default=None,
        help="Force usage of specified encoding is possible",
    )

    return subparser


def add_args(subparser: Any):
    parser = subparser.add_parser("download", help="Download data from Common Crawl")
    parser.add_argument("url", type=str, help="URL to query")
    parser.add_argument("output", type=Path, help="Path to output directory")
    mode_subparser = parser.add_subparsers(
        dest="mode", required=True, help="Download mode"
    )
    mode_subparser = add_mode_args(mode_subparser)
    parser.add_argument(
        "--limit", type=int, default=5, help="Max number of urls to download"
    )
    parser.add_argument(
        "--since",
        type=datetime.fromisoformat,
        default=str(datetime.min),
        help="Start date in ISO format e.g. 2020-01-01",
    )
    parser.add_argument(
        "--to",
        type=datetime.fromisoformat,
        default=str(datetime.max),
        help="End date in ISO format e.g. 2020-01-01",
    )
    parser.add_argument(
        "--cc_server",
        nargs="+",
        type=str,
        default=None,
        help="Common Crawl indexes to query, must provide whole url e.g. https://index.commoncrawl.org/CC-MAIN-2023-14-index",
    )
    parser.add_argument(
        "--max_retry",
        type=int,
        default=30,
        help="Max number of retries for a request, when the requests are failing increase this number",
    )
    parser.add_argument(
        "--sleep_step",
        type=int,
        default=4,
        help="Number of increased second to add to sleep time between each failed download attempt, increase this number if the server tell you to slow down",
    )
    # Add option to output to either json or html
    parser.add_argument(
        "--match_type",
        type=MatchType,
        choices=list(MatchType),
        help="Match type for the url, see cdx-api for more info",
    )
    parser.add_argument(
        "--max_directory_size",
        type=int,
        default=None,
        help="Max number of files per directory",
    )
    parser.add_argument(
        "--filter_non_200",
        action="store_true",
        default=True,
        help="Filter out non 200 status code",
    )
    parser.set_defaults(func=run_download)


def url_download_prepare_router(
    output_format: DownloadOutputFormat, filter_non_200: bool, encoding: str | None
):
    router = Router()
    match output_format:
        case DownloadOutputFormat.RECORD:
            router.load_extractor(
                "dummy_extractor", DomainRecordExtractor(filter_non_ok=filter_non_200)
            )
        case DownloadOutputFormat.HTML:
            router.load_extractor(
                "dummy_extractor",
                HTMLExtractor(filter_non_ok=filter_non_200, encoding=encoding),
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
    match_type: MatchType | None,
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
    encoding: str | None,
):
    outstreamer = url_download_prepare_streamer(
        mode, output, max_directory_size, max_crawls_per_file
    )
    router = url_download_prepare_router(mode, filter_non_200, encoding)
    pipeline = ProcessorPipeline(
        router, AsyncDownloader(max_retry=max_retry), outstreamer
    )

    index_agg = IndexAggregator(
        cc_servers=cc_server or [],
        domains=[url],
        match_type=match_type,
        since=since,
        to=to,
        limit=limit,
        max_retry=max_retry,
        sleep_step=sleep_step,
    )
    await query_and_extract(index_agg, pipeline)


def run_download(args: argparse.Namespace):
    mode = DownloadOutputFormat(args.mode)
    return asyncio.run(
        url_download(
            url=args.url,
            match_type=args.match_type,
            output=args.output,
            cc_server=args.cc_server,
            since=args.since,
            to=args.to,
            limit=args.limit,
            max_retry=args.max_retry,
            sleep_step=args.sleep_step,
            mode=mode,
            max_crawls_per_file=args.max_crawls_per_file
            if mode == DownloadOutputFormat.RECORD
            else 1,
            max_directory_size=args.max_directory_size,
            filter_non_200=args.filter_non_200,
            encoding=args.encoding if mode == DownloadOutputFormat.HTML else None,
        )
    )
