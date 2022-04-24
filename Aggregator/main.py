from index_query import DomainIndexer, Domain_Record
from reddis_helpers import create_pool
import aioredis



async def main():
    redis_conn = create_pool("redis://localhost:6379")
    async with DomainIndexer(["idnes.cz"]) as klldi:
        async for record in di:
            if 



if __name__ == "__main__":
    asyncio.run(main())
