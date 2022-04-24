from index_query import DomainIndexer, Domain_Record
from reddis_helpers import create_pool
import aioredis

async def process_and_forward(record: Domain_Record, redis_conn:aioredis.Redis ) -> None:
    redis_conn.get

    if redis_conn.setnx(record.filename)



async def main():
    redis_conn = create_pool("redis://localhost:6379")
    async with DomainIndexer(["idnes.cz"]) as klldi:
        async for record in di:
            if 



if __name__ == "__main__":
    asyncio.run(main())
