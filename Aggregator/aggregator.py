import argparse
import asyncio
from datetime import datetime
import json
import logging
from typing import List

from stomp import Connection
from stomp.exception import StompException

from index_query import IndexAggregator

logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - " "%(levelname)s - %(message)s",
    level="INFO",
)

DUPL_ID_HEADER = "_AMQ_DUPL_ID"


def init_connection(conn: Connection):
    conn.connect(login="producer", passcode="producer", wait=True)
    return conn


async def aggregate(
    url: str,
    queue_host: str,
    queue_port: int,
    cc_servers: List[str],
    since: datetime,
    to: datetime,
    limit: int | None,
    max_retries: int,
    prefetch_size: int,
    sleep_step: int,
):
    conn = Connection([(queue_host, queue_port)], heartbeats=(10000, 10000))
    logging.info(f"Connected to queue at: {queue_host}:{queue_port}")
    i = 0
    async with IndexAggregator(
        [url],
        cc_servers=cc_servers,
        since=since,
        to=to,
        limit=limit,
        prefetch_size=prefetch_size,
        max_retries=max_retries,
        sleep_step=sleep_step,
    ) as aggregator:
        async for domain_record in aggregator:
            try:
                if not conn.is_connected():
                    conn = init_connection(conn)
                json_str = json.dumps(domain_record.__dict__, default=str)
                conn.send(
                    f"/queue/articles.{url}",
                    json_str,
                    headers={DUPL_ID_HEADER: domain_record.url},
                )
                logging.info(f"Sent url: {domain_record.url}")
                i += 1
            except (KeyboardInterrupt, StompException) as e:
                logging.error(e)
                break

    logging.info("Sent {i} messages")
    conn.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("url", type=str)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--since", type=str, default=datetime.min)
    parser.add_argument("--to", type=str, default=datetime.max)
    parser.add_argument("--queue_host", type=str, default="artemis")
    parser.add_argument("--queue_port", type=int, default=61613)
    parser.add_argument("--cc_servers", nargs="+", type=str, default=[])
    parser.add_argument("--prefetch_size", type=int, default=3)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--sleep_step", type=int, default=1)
    args = parser.parse_args()
    asyncio.run(aggregate(**vars(args)))
