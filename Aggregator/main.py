from distutils.log import error
import time
from Aggregator.index_query import IndexAggregator
import asyncio


async def main():
    records = []

    async def dumb_aggregator(dr):
        records.append(dr)
        return 1

    cc_servers = [
        "https://index.commoncrawl.org/CC-MAIN-2022-21-index",
        "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
        "https://index.commoncrawl.org/CC-MAIN-2021-49-index",
        "https://index.commoncrawl.org/CC-MAIN-2021-43-index",
        "https://index.commoncrawl.org/CC-MAIN-2021-39-index",
        "https://index.commoncrawl.org/CC-MAIN-2021-31-index",
    ]

    async with IndexAggregator(["idnes.cz"], cc_servers=cc_servers) as di:
        st = time.time()
        try:
            await di.aggregate(dumb_aggregator, max_retries=9999)
        except Exception as e:
            print(type(e))

        print(f"First test {time.time() - st}")

        st = time.time()
        await asyncio.sleep(20)
        print("Starting second test")
        async for record in di:
            await dumb_aggregator(di)
        print(f"Second test {time.time() - st}")


asyncio.run(main())
