from datetime import datetime
from enum import Enum
import json
import logging
import multiprocessing
from pathlib import Path
from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.common.types import ExtractConfig

from cmoncrawl.processor.pipeline.downloader import DownloaderDummy, AsyncDownloader
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.processor.pipeline.streamer import StreamerFileHTML
from cmoncrawl.processor.pipeline.extractor import HTMLExtractor, DomainRecordExtractor
from cmoncrawl.middleware.synchronized import index_and_extract, extract
import argparse
from typing import Any, Dict, List
import asyncio
from cmoncrawl.processor.pipeline.streamer import (
    StreamerFileJSON,
)

from cmoncrawl.processor.pipeline.router import Router
from cmoncrawl.common.loggers import (
    all_purpose_logger,
    metadata_logger,
)
from cmoncrawl.common.types import DomainRecord


# define enum
class DownloadOutputFormat(Enum):
    RECORD = "records"
    HTML = "html"


class ExtractMode(Enum):
    HTML = "html_extract"
    RECORD = "record_extract"


def setup_loggers(debug: bool):
    if debug:
        all_purpose_logger.setLevel(logging.DEBUG)
        metadata_logger.setLevel(logging.DEBUG)
    else:
        all_purpose_logger.setLevel(logging.INFO)
        metadata_logger.setLevel(logging.INFO)


def get_extract_downloader(
    mode: ExtractMode, files_path: List[Path], url: str | None, date: datetime | None
):
    match mode:
        case ExtractMode.HTML:
            return DownloaderDummy(files_path, url, date)
        case ExtractMode.RECORD:
            return AsyncDownloader()


def get_domain_records_json(file_path: Path) -> List[DomainRecord]:
    with open(file_path, "r") as f:
        js = [json.loads(line) for line in f.readlines()]
    return [DomainRecord.schema().load(record["domain_record"]) for record in js]


def get_domain_records_html(url: str | None, date: datetime | None):
    # Just return dummy as correct crawl will be loaded from dummy downloader
    return [DomainRecord("", url=url, offset=0, length=0, timestamp=date)]


def load_config(config_path: Path) -> ExtractConfig:
    with open(config_path, "r") as f:
        config = json.load(f)
    return ExtractConfig.schema().load(config)


def create_router(config: ExtractConfig) -> Router:
    router = Router()
    router.load_modules(config.extractors_path)
    router.register_routes(config.routes)
    return router


async def extract_from_files(
    files: List[Path],
    output_path: Path,
    config: ExtractConfig,
    mode: ExtractMode,
    url: str | None,
    date: datetime | None,
    max_directory_size: int,
    max_crawls_per_file: int,
    debug: bool,
):
    setup_loggers(debug)
    router = create_router(config)
    downloader = get_extract_downloader(mode, files, url, date)
    outstreamer = StreamerFileJSON(output_path, max_directory_size, max_crawls_per_file)
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    for path in files:
        match mode:
            case ExtractMode.RECORD:
                domain_records = get_domain_records_json(path)
            case ExtractMode.HTML:
                domain_records = get_domain_records_html(url, date)
        await extract(domain_records, pipeline)


def extract_args():
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("output_path", type=Path)
    parser.add_argument(
        "config_path",
        type=Path,
    )
    parser.add_argument(
        "--mode", type=ExtractMode, choices=list(ExtractMode), default=ExtractMode.HTML
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--date", type=str, default=datetime.now().isoformat())
    parser.add_argument("--max_crawls_per_file", type=int, default=100_000)
    parser.add_argument("--max_directory_size", type=int, default=1000)
    parser.add_argument("--n_proc", type=int, default=1)
    parser.add_argument("--url", type=str, default="")
    return vars(parser.parse_args())


def _extract_task(
    output_path: Path, config: ExtractConfig, files: List[Path], args: Dict[str, Any]
):
    asyncio.run(
        extract_from_files(output_path=output_path, config=config, files=files, **args)
    )


def run_extract():
    args = extract_args()
    args["date"] = datetime.fromisoformat(args["date"])
    files = args["files"]
    del args["files"]
    pool = multiprocessing.Pool(args["n_proc"])
    del args["n_proc"]
    output_path = args["output_path"]
    del args["output_path"]
    config = load_config(args["config_path"])
    del args["config_path"]
    pool.starmap(
        _extract_task,
        [(output_path / str(i), config, [file], args) for i, file in enumerate(files)],
    )


# ==================================================================================================


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
    cc_server: str,
    since: datetime,
    to: datetime,
    limit: int,
    max_retry: int,
    output_type: DownloadOutputFormat,
    max_crawls_per_file: int,
    max_directory_size: int,
    filter_non_200: bool,
    debug: bool,
):
    setup_loggers(debug)
    outstreamer = url_download_prepare_streamer(
        output_type, output, max_directory_size, max_crawls_per_file
    )
    router = url_download_prepare_router(output_type, filter_non_200)
    pipeline = ProcessorPipeline(
        router, AsyncDownloader(max_retry=max_retry), outstreamer
    )

    index_agg = IndexAggregator(
        cc_servers=([cc_server] if cc_server else []),
        domains=[url],
        since=since,
        to=to,
        limit=limit,
        max_retry=max_retry,
    )
    await index_and_extract(index_agg, pipeline)


def run_download():
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("url")
    parser.add_argument("output", type=Path)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--since", type=str, default=datetime.min)
    parser.add_argument("--to", type=str, default=datetime.max)
    parser.add_argument("--cc_server", type=str, default=None)
    parser.add_argument("--max_retry", type=int, default=30)
    # Add option to output to either json or html
    parser.add_argument(
        "--output_type",
        type=DownloadOutputFormat,
        choices=list(DownloadOutputFormat),
        default=DownloadOutputFormat.RECORD,
    )
    parser.add_argument("--max_directory_size", type=int, default=1000)
    parser.add_argument("--filter_non_200", action="store_true", default=True)
    json_group = parser.add_argument_group("json")
    json_group.add_argument("--max_crawls_per_file", type=int, default=100_000)
    args = vars(parser.parse_args())
    if isinstance(args["since"], str):
        args["since"] = datetime.fromisoformat(args["since"])

    if isinstance(args["to"], str):
        args["to"] = datetime.fromisoformat(args["to"])

    asyncio.run(url_download(**args))
