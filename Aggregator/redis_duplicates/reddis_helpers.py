import aioredis
import time
from Aggregator.index_query import Domain_Record
from Aggregator.distributed_queue import Queue


async def process_and_forward(
    redis_conn: aioredis.Redis, queue: Queue[Domain_Record], record: Domain_Record
) -> int | None:
    # Already set
    if await redis_conn.get(record.url) is not None:
        return None

    # Try set if not already set
    # Which could have happened if another process set it
    cur_time = int(time.time())
    if await redis_conn.setnx(record.url, cur_time) is None:
        return None

    queue.enqueue(record)
    return cur_time


def create_connection(url: str):
    return aioredis.from_url(url)
