from datetime import datetime
import unittest
from collections import deque
from Aggregator.index_query import DomainRecord
from Aggregator.distributed_queue import DummyQueue
from Aggregator.redis_duplicates.reddis_helpers import (
    process_and_forward,
    create_connection,
)


class TestRedisAynsc(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis_conn = create_connection("redis://redis-duplicates:6379")
        await self.redis_conn.flushall()

    async def test_process_and_forward(self):

        record = DomainRecord("x", "idnes.cz", 0, 1, datetime.now())
        queue: DummyQueue[DomainRecord] = DummyQueue()
        await process_and_forward(self.redis_conn, queue, record)
        await process_and_forward(self.redis_conn, queue, record)
        record2 = DomainRecord("x", "seznam.cz", 0, 1, datetime.now())
        await process_and_forward(self.redis_conn, queue, record2)
        self.assertEqual(queue.queue, deque([record2, record]))
