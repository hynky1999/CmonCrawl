import argparse
import asyncio
from datetime import datetime
import json
import logging
import re
from typing import List
from urllib.parse import urlparse

from stomp import Connection
from stomp.exception import StompException

from Aggregator.App.index_query import IndexAggregator
from Aggregator.App.utils import all_purpose_logger

DUPL_ID_HEADER = "_AMQ_DUPL_ID"
logging.basicConfig(level=logging.WARN)
all_purpose_logger.setLevel(logging.INFO)


def init_connection(conn: Connection):
    conn.connect(login="producer", passcode="producer", wait=True)
    all_purpose_logger.info(f"Connected to queue")
    return conn


ext_sub = re.compile(r"\.html|\.jpg|\.png|\.zip")
multiple_slash = re.compile(r"/")
path_re = re.compile(r"(\/[a-zA-Z0-9_\-]*)*(\/[a-zA-Z0-9\-]*)")
remove_trailing = re.compile(r"[\/\-0-9]+$")


def unify_url_id(url: str):
    parsed = urlparse(url)
    path_processed = ext_sub.sub("", parsed.path)
    path_processed = multiple_slash.sub("/", path_processed)
    path_match = path_re.search(path_processed)
    if path_match:
        path_processed = path_match.group(0)
    else:
        all_purpose_logger.warning(f"No path match for {url}")
        path_processed = ""
    path_processed = remove_trailing.sub("", path_processed)
    netloc = parsed.netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]

    return f"{netloc}{path_processed}"


async def aggregate(
    url: str,
    queue_host: str,
    queue_port: int,
    cc_servers: List[str],
    since: datetime,
    to: datetime,
    limit: int | None,
    max_retry: int,
    prefetch_size: int,
    sleep_step: int,
):
    conn = Connection([(queue_host, queue_port)], heartbeats=(10000, 10000))
    # make sure we have connection so that we cant send the poission pill
    while conn.is_connected() is False:
        try:
            conn = init_connection(conn)
        except StompException:
            pass
    i = 0
    async with IndexAggregator(
        [url],
        cc_servers=cc_servers,
        since=since,
        to=to,
        limit=limit,
        prefetch_size=prefetch_size,
        max_retry=max_retry,
        sleep_step=sleep_step,
    ) as aggregator:
        async for domain_record in aggregator:
            try:
                while not conn.is_connected():
                    conn = init_connection(conn)

                json_str = json.dumps(domain_record.__dict__, default=str)
                id = unify_url_id(domain_record.url)
                conn.send(
                    f"queue.{url}",
                    json_str,
                    headers={DUPL_ID_HEADER: id},
                )
                all_purpose_logger.debug(f"Sent url: {domain_record.url} with id: {id}")
                i += 1
            except (KeyboardInterrupt) as e:
                break
            except Exception as e:
                all_purpose_logger.error(e, exc_info=True)

    all_purpose_logger.info(f"Sent {i} messages")
    conn.send(f"topic.poisson_pill.{url}", "", headers={"type": "poisson_pill"})
    conn.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("url", type=str)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--since", type=str, default=datetime.min)
    parser.add_argument("--to", type=str, default=datetime.max)
    parser.add_argument("--queue_host", type=str, default="artemis")
    parser.add_argument("--queue_port", type=int, default=61616)
    parser.add_argument("--cc_servers", nargs="+", type=str, default=[])
    parser.add_argument("--prefetch_size", type=int, default=2)
    parser.add_argument("--max_retry", type=int, default=20)
    parser.add_argument("--sleep_step", type=int, default=5)
    args = vars(parser.parse_args())
    if isinstance(args["since"], str):
        args["since"] = datetime.fromisoformat(args["since"])

    if isinstance(args["to"], str):
        args["to"] = datetime.fromisoformat(args["to"])
    asyncio.run(aggregate(**args))
