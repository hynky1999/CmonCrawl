import argparse
import logging
from typing import Any, Callable, Dict
from cmoncrawl.integrations.download import add_args as add_download_args
from cmoncrawl.integrations.extract import add_args as add_extract_args
from cmoncrawl.common.loggers import (
    all_purpose_logger,
    metadata_logger,
)


def add_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--verbosity", "-v", action="count", default=0, help="Increase verbosity"
    )
    return parser


def add_subparsers(parser: Any):
    parsers: Dict[str, argparse.ArgumentParser] = {}
    for add_args_fc in [add_download_args, add_extract_args]:
        add_args_fc(parser)
    return parsers


def get_args():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(
        dest="command", required=True, help="Command to run"
    )
    add_subparsers(subparser)
    return parser


def setup_loggers(verbosity: int):
    verbosity_cfg = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
    }
    verbosity = min(verbosity, max(verbosity_cfg.keys()))
    all_purpose_logger.setLevel(verbosity_cfg[verbosity])
    metadata_logger.setLevel(verbosity_cfg[verbosity])


def process_args(args: argparse.Namespace):
    setup_loggers(args.verbosity)


def main():
    parser = get_args()
    parser = add_args(parser)
    args = parser.parse_args()
    process_args(args)
    all_purpose_logger.debug(f"Args: {args}")
    args.func(args)


if __name__ == "__main__":
    main()
