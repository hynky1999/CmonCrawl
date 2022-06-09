from datetime import datetime
from os import path
import sys
from typing import List
from Aggregator.constants import MAX_DATE, MIN_DATE
from Processor.Downloader.download import Downloader
from Aggregator.index_query import DomainRecord, IndexAggregator
import asyncio

FOLDER = "articles_downloaded"


async def main(url: str, cc_server: List[str] =[], since: datetime=MIN_DATE, to: datetime=MAX_DATE):
    # Sync queue because I didn't want to put effort in
    records: List[DomainRecord] = []
    async with IndexAggregator(
        [url], cc_servers=cc_server, since=since, to=to,
    ) as aggregator:

        async for domain_record in aggregator:
            records.append(domain_record)
        
    async with Downloader(digest_verification=True)  as downloader, OutStrea:


    








if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 download_article.py <url>")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
