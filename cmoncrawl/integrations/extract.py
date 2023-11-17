import argparse
import asyncio
import json
import multiprocessing
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Tuple

from tqdm import tqdm

from cmoncrawl.common.loggers import setup_loggers
from cmoncrawl.common.types import DomainRecord, ExtractConfig
from cmoncrawl.config import CONFIG
from cmoncrawl.integrations.utils import DAOname, get_dao
from cmoncrawl.middleware.synchronized import extract
from cmoncrawl.processor.connectors.base import ICC_Dao
from cmoncrawl.processor.pipeline.downloader import (
    AsyncDownloader,
    DownloaderLocalFiles,
)
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.processor.pipeline.router import Router
from cmoncrawl.processor.pipeline.streamer import (
    StreamerFileJSON,
)


class ExtractMode(Enum):
    HTML = "html"
    RECORD = "record"


def add_mode_args(subparser: Any):
    record_parser = subparser.add_parser(
        ExtractMode.RECORD.value, help="Extract data from jsonl record files"
    )
    record_parser.add_argument(
        "--max_retry",
        type=int,
        default=30,
        help="Max number of warc download attempts",
    )
    record_parser.add_argument(
        "--download_method",
        type=DAOname,
        default=DAOname.API,
        choices=[e for e in DAOname],
        help="Method of downloading warc files",
    )

    record_parser.add_argument(
        "--sleep_base",
        type=float,
        default=1.5,
        help="Base value for exponential backoff between failed requests",
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
    sleep_base: float,
    dao: ICC_Dao | None,
):
    match mode:
        case ExtractMode.HTML:
            return DownloaderLocalFiles(files_path, url, date)
        case ExtractMode.RECORD:
            if dao is None:
                raise ValueError("DAO must be specified for record extraction")

            return AsyncDownloader(max_retry=max_retry, sleep_base=sleep_base, dao=dao)


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
        (
            DomainRecord(filename="", url=url, offset=0, length=0, timestamp=date),
            {},
        )
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
    sleep_base: float,
    download_method: DAOname | None,
):
    router = create_router(config)
    outstreamer = StreamerFileJSON(output_path, max_directory_size, max_crawls_per_file)
    dao = get_dao(download_method)
    try:
        if dao is not None:
            await dao.__aenter__()
        downloader = get_extract_downloader(
            mode, files, url, date, max_retry, sleep_base, dao
        )
        pipeline = ProcessorPipeline(router, downloader, outstreamer)
        for path in files:
            match mode:
                case ExtractMode.RECORD:
                    records = get_domain_records_json(path)
                case ExtractMode.HTML:
                    records = get_domain_records_html(url, date)
            await extract(records, pipeline)
    finally:
        if dao is not None:
            await dao.__aexit__(None, None, None)


def _extract_task(
    output_path: Path,
    config: ExtractConfig,
    files: List[Path],
    args: argparse.Namespace,
):
    mode = ExtractMode(args.mode)
    download_method = DAOname(args.download_method) if args.download_method else None

    # We have to setup loggers / aws in each process
    setup_loggers(args.verbosity)
    CONFIG.update_from_cli(args)

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
            sleep_base=args.sleep_base if mode == ExtractMode.RECORD else 0,
            download_method=download_method,
        )
    )


def run_extract(args: argparse.Namespace):
    config = load_config(args.config_path)
    pool = multiprocessing.Pool(args.n_proc)
    pool.starmap(
        _extract_task,
        [
            (
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
