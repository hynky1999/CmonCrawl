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
        "--verbosity", action="store_true", default=False, help="Debug mode"
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


def setup_loggers(debug: bool):
    if debug:
        all_purpose_logger.setLevel(logging.DEBUG)
        metadata_logger.setLevel(logging.DEBUG)
    else:
        all_purpose_logger.setLevel(logging.INFO)
        metadata_logger.setLevel(logging.INFO)


def process_args(args: argparse.Namespace):
    setup_loggers(args.debug)


def main():
    parser = get_args()
    parser = add_args(parser)
    args = parser.parse_args()
    process_args(args)
    all_purpose_logger.debug(f"Args: {args}")
    args.func(args)


if __name__ == "__main__":
    main()
