from datetime import datetime
import time
from Aggregator.index_query import IndexAggregator
import asyncio


async def main():

    cc_servers = [
        "https://index.commoncrawl.org/CC-MAIN-2022-21-index",
        # "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
        # "https://index.commoncrawl.org/CC-MAIN-2021-49-index",
        # "https://index.commoncrawl.org/CC-MAIN-2021-43-index",
        # "https://index.commoncrawl.org/CC-MAIN-2021-39-index",
        # "https://index.commoncrawl.org/CC-MAIN-2021-31-index",
        # "https://index.commoncrawl.org/CC-MAIN-2008-2009-index"
    ]

    async with IndexAggregator(
        ["idnes.cz"],
        cc_servers=cc_servers,
        since=datetime(2020, 1, 1),
        to=datetime(2020, 1, 2),
    ) as di:
        st = time.time()
        print("Starting second test")
        list = []
        async for record in di:
            list.append(record)

        print(f"Second test {time.time() - st}")
        print(len(list))


asyncio.run(main(), debug=True)
