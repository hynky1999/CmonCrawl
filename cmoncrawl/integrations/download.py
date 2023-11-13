from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, List
from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.common.types import MatchType
from cmoncrawl.integrations.utils import DAOname, get_dao
from cmoncrawl.processor.connectors.base import ICC_Dao
from cmoncrawl.processor.pipeline.downloader import (
    AsyncDownloader,
    DummyDownloader,
)
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.processor.pipeline.streamer import StreamerFileHTML
from cmoncrawl.processor.pipeline.extractor import (
    HTMLExtractor,
    DomainRecordExtractor,
)
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
        DownloadOutputFormat.HTML.value,
        help="Download HTML files from Common Crawl",
    )

    html_parser.add_argument(
        "--encoding",
        type=str,
        default=None,
        help="Force usage of specified encoding is possible",
    )
    html_parser.add_argument(
        "--download_method",
        type=DAOname,
        default=DAOname.API,
        choices=[e for e in DAOname],
        help="Method for downloading warc files from Common Crawl, it only applies to HTML download",
    )

    return subparser


def add_args(subparser: Any):
    parser = subparser.add_parser(
        "download", help="Download data from Common Crawl"
    )
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
        "--sleep_base",
        type=int,
        default=1.5,
        help="Base sleep time for exponential backoff in case of request failure.",
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
    output_format: DownloadOutputFormat,
    filter_non_200: bool,
    encoding: str | None,
):
    router = Router()
    match output_format:
        case DownloadOutputFormat.RECORD:
            router.load_extractor(
                "dummy_extractor",
                DomainRecordExtractor(filter_non_ok=filter_non_200),
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
            return StreamerFileHTML(
                root=output, max_directory_size=max_direrctory_size
            )


def get_download_downloader(
    output_format: DownloadOutputFormat,
    max_retry: int,
    sleep_base: float,
    dao: ICC_Dao | None,
):
    match output_format:
        case DownloadOutputFormat.HTML:
            if dao is None:
                raise ValueError("DAO must be specified for record extraction")

            return AsyncDownloader(
                max_retry=max_retry, sleep_base=sleep_base, dao=dao
            )
        case DownloadOutputFormat.RECORD:
            return DummyDownloader()


async def url_download(
    url: str,
    match_type: MatchType | None,
    output: Path,
    cc_server: List[str] | None,
    since: datetime,
    to: datetime,
    limit: int,
    max_retry: int,
    sleep_base: float,
    mode: DownloadOutputFormat,
    max_crawls_per_file: int,
    max_directory_size: int,
    filter_non_200: bool,
    encoding: str | None,
    download_method: DAOname | None,
):
    outstreamer = url_download_prepare_streamer(
        mode, output, max_directory_size, max_crawls_per_file
    )
    router = url_download_prepare_router(mode, filter_non_200, encoding)
    dao = get_dao(download_method)

    try:
        if dao is not None:
            await dao.__aenter__()

        downloader = get_download_downloader(mode, max_retry, sleep_base, dao)
        pipeline = ProcessorPipeline(router, downloader, outstreamer)
        index_agg = IndexAggregator(
            cc_servers=cc_server or [],
            domains=[url],
            match_type=match_type,
            since=since,
            to=to,
            limit=limit,
            max_retry=max_retry,
            sleep_base=sleep_base,
        )
        await query_and_extract(index_agg, pipeline)
    finally:
        if dao is not None:
            await dao.__aexit__(None, None, None)


def run_download(args: argparse.Namespace):
    mode = DownloadOutputFormat(args.mode)
    download_method = (
        DAOname(args.download_method)
        if mode == DownloadOutputFormat.HTML
        else None
    )
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
            sleep_base=args.sleep_base,
            mode=mode,
            max_crawls_per_file=args.max_crawls_per_file
            if mode == DownloadOutputFormat.RECORD
            else 1,
            max_directory_size=args.max_directory_size,
            filter_non_200=args.filter_non_200,
            encoding=args.encoding
            if mode == DownloadOutputFormat.HTML
            else None,
            download_method=download_method,
        )
    )
