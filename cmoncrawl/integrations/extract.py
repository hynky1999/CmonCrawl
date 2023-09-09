from datetime import datetime
from enum import Enum
import json
import multiprocessing
from pathlib import Path

from tqdm import tqdm
from cmoncrawl.common.loggers import setup_loggers
from cmoncrawl.common.types import ExtractConfig

from cmoncrawl.processor.pipeline.downloader import DownloaderDummy, AsyncDownloader
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.middleware.synchronized import extract
import argparse
from typing import Any, Dict, List, Tuple
import asyncio
from cmoncrawl.processor.pipeline.streamer import (
    StreamerFileJSON,
)

from cmoncrawl.processor.pipeline.router import Router
from cmoncrawl.common.types import DomainRecord


class ExtractMode(Enum):
    HTML = "html"
    RECORD = "record"


def add_mode_args(subparser: Any):
    record_parser = subparser.add_parser(
        ExtractMode.RECORD.value, help="Extract data from jsonl record files"
    )
    record_parser.add_argument(
        "--max_retry", type=int, default=30, help="Max number of warc download attempts"
    )
    record_parser.add_argument(
        "--sleep_step",
        type=int,
        default=4,
        help="Number of increased second to add to sleep time between each failed download attempt",
    )

    html_parser = subparser.add_parser(
        ExtractMode.HTML.value, help="Extract data from HTML files"
    )
    html_parser.add_argument(
        "--date",
        type=datetime.fromisoformat,
        default=str(datetime.now()),
        help="Date of extraction of HTML files in iso format e.g. 2021-01-01, default is today",
    )
    html_parser.add_argument(
        "--url",
        type=str,
        default="",
        help="URL from which the HTML files were downloaded, by default it will try to infer from file content",
    )
    return subparser


def add_args(subparser: Any):
    parser = subparser.add_parser(
        "extract", help="Extract data from records/html files"
    )
    parser.add_argument(
        "config_path",
        type=Path,
        help="Path to config file containing extraction rules",
    )
    parser.add_argument("output_path", type=Path, help="Path to output directory")
    parser.add_argument(
        "files", nargs="+", type=Path, help="Files to extract data from"
    )
    parser.add_argument(
        "--max_crawls_per_file",
        type=int,
        default=500_000,
        help="Max number of extractions per file output",
    )
    parser.add_argument(
        "--max_directory_size",
        type=int,
        default=None,
        help="Max number of extraction files per directory",
    )
    parser.add_argument(
        "--n_proc",
        type=int,
        default=1,
        help="Number of processes to use for extraction. The paralelization is on file level, thus for single file it's useless to use more than one process.",
    )

    mode_subparser = parser.add_subparsers(
        dest="mode", required=True, help="Extraction mode"
    )
    mode_subparser = add_mode_args(mode_subparser)
    parser.set_defaults(func=run_extract)


def get_extract_downloader(
    mode: ExtractMode,
    files_path: List[Path],
    url: str | None,
    date: datetime | None,
    max_retry: int,
    sleep_step: int,
):
    match mode:
        case ExtractMode.HTML:
            return DownloaderDummy(files_path, url, date)
        case ExtractMode.RECORD:
            return AsyncDownloader(max_retry=max_retry, sleep_step=sleep_step)


def get_domain_records_json(
    file_path: Path,
) -> List[Tuple[DomainRecord, Dict[str, Any]]]:
    records: List[Tuple[DomainRecord, Dict[str, Any]]] = []
    with open(file_path, "r") as f:
        for line in tqdm(f):
            js = json.loads(line)
            domain_record: DomainRecord = DomainRecord.model_validate(
                js["domain_record"]
            )
            additional_info = js.get("additional_info", {})
            if not isinstance(additional_info, dict):
                additional_info = {}
            records.append((domain_record, additional_info))  # type: ignore
    return records


def get_domain_records_html(
    url: str | None, date: datetime | None
) -> List[Tuple[DomainRecord, Dict[str, Any]]]:
    # Just return dummy as correct crawl will be loaded from dummy downloader
    return [
        (DomainRecord(filename="", url=url, offset=0, length=0, timestamp=date), {})
    ]


def load_config(config_path: Path) -> ExtractConfig:
    with open(config_path, "r") as f:
        config = json.load(f)
    return ExtractConfig.model_validate(config)


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
    max_retry: int,
    sleep_step: int,
):
    router = create_router(config)
    downloader = get_extract_downloader(mode, files, url, date, max_retry, sleep_step)
    outstreamer = StreamerFileJSON(output_path, max_directory_size, max_crawls_per_file)
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    for path in files:
        match mode:
            case ExtractMode.RECORD:
                records = get_domain_records_json(path)
            case ExtractMode.HTML:
                records = get_domain_records_html(url, date)
        await extract(records, pipeline)


def _extract_task(
    output_path: Path,
    config: ExtractConfig,
    files: List[Path],
    args: argparse.Namespace,
):
    mode = ExtractMode(args.mode)
    setup_loggers(args.verbosity)

    asyncio.run(
        extract_from_files(
            output_path=output_path,
            config=config,
            files=files,
            mode=mode,
            url=args.url if mode == ExtractMode.HTML else None,
            date=args.date if mode == ExtractMode.HTML else None,
            max_directory_size=args.max_directory_size,
            max_crawls_per_file=args.max_crawls_per_file,
            max_retry=args.max_retry if mode == ExtractMode.RECORD else 0,
            sleep_step=args.sleep_step if mode == ExtractMode.RECORD else 0,
        )
    )


def run_extract(args: argparse.Namespace):
    config = load_config(args.config_path)
    pool = multiprocessing.Pool(args.n_proc)
    pool.starmap(
        _extract_task,
        [
            (
                # [metadata_logger.level, all_purpose_logger.level],
                args.output_path / f"{file.stem}"
                if args.n_proc != 1
                else args.output_path,
                config,
                [file],
                args,
            )
            for _, file in enumerate(args.files)
        ],
    )
