import index_query
import asyncio


async def __main__():
    async with index_query.DomainIndexer([]) as di:
        print(1)


asyncio.run(__main__())