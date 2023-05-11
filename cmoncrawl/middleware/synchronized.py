from pathlib import Path
from typing import List, Set
from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.common.types import DomainRecord
from cmoncrawl.common.loggers import all_purpose_logger, metadata_logger
from cmoncrawl.aggregator.utils.helpers import unify_url_id
from tqdm import tqdm
import asyncio


async def index_and_extract(
    index_agg: IndexAggregator,
    pipeline: ProcessorPipeline,
    filter_non_unique_url: bool = False,
):
    processed_urls: Set[str] = set()

    if hasattr(pipeline.downloader, "__aenter__"):
        await pipeline.downloader.__aenter__()
    try:
        async with index_agg:
            async for domain_record in index_agg:
                url = domain_record.url or ""
                if filter_non_unique_url and unify_url_id(url) in processed_urls:
                    continue
                try:
                    await pipeline.process_domain_record(domain_record)
                except KeyboardInterrupt as e:
                    break

                except Exception as e:
                    all_purpose_logger.error(
                        f"Failed to process {domain_record.url} with {e}"
                    )
                    continue
                processed_urls.add(unify_url_id(url))

    finally:
        if hasattr(pipeline.downloader, "__aexit__"):
            await pipeline.downloader.__aexit__(None, None, None)


async def _extract_task(domain_record: DomainRecord, pipeline: ProcessorPipeline):
    result = []
    try:
        result = await pipeline.process_domain_record(domain_record)
    except KeyboardInterrupt as e:
        raise e
    except Exception as e:
        metadata_logger.error(
            f"Failed to process {domain_record.url} with {e}",
            extra={"domain_record": domain_record},
        )
    return result


async def extract(
    domain_records: List[DomainRecord],
    pipeline: ProcessorPipeline,
    concurrent_length: int = 20,
    timeout: int = 5,
):
    domain_records_iterator = iter(tqdm(domain_records))
    domains_exausted = False
    if hasattr(pipeline.downloader, "__aenter__"):
        await pipeline.downloader.__aenter__()
    try:
        queue: Set[asyncio.Task[List[Path]]] = set()
        while not domains_exausted or len(queue) > 0:
            # Put into queue till possible
            while len(queue) < concurrent_length and not domains_exausted:
                next_domain_record = next(domain_records_iterator, None)
                if next_domain_record is None:
                    domains_exausted = True
                    break

                queue.add(
                    asyncio.create_task(_extract_task(next_domain_record, pipeline))
                )

            done, queue = await asyncio.wait(
                queue, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    await task
                except KeyboardInterrupt as e:
                    break

                except Exception as _:
                    all_purpose_logger.error(f"Failed to process {task}")
                    pass
    except Exception as e:
        all_purpose_logger.error(e, exc_info=True)

    finally:
        if hasattr(pipeline.downloader, "__aexit__"):
            await pipeline.downloader.__aexit__(None, None, None)
