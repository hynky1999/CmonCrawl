import asyncio
import logging
from typing import Any, Dict, List, Set, Tuple

from tqdm import tqdm

from cmoncrawl.aggregator.base import IAggregator
from cmoncrawl.aggregator.utils.helpers import unify_url_id
from cmoncrawl.common.loggers import all_purpose_logger, metadata_logger
from cmoncrawl.common.types import DomainRecord
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline


async def query_and_extract(
    index_agg: IAggregator,
    pipeline: ProcessorPipeline,
    filter_non_unique_url: bool = False,
):
    """
    Query the index and extracts the results using the pipeline

    Args:
        index_agg (IndexAggregator): Index aggregator
        pipeline (ProcessorPipeline): Pipeline to use
        filter_non_unique_url (bool, optional): Filter non unique urls.
            if True, only first successful extraction of a url will be processed,
            the rest will be skipped. Defaults to False.

    """
    processed_urls: Set[str] = set()
    total_extracted: int = 0

    async with index_agg:
        async for domain_record in index_agg:
            url = domain_record.url or ""
            if filter_non_unique_url and unify_url_id(url) in processed_urls:
                continue
            try:
                await pipeline.process_domain_record(domain_record, {})
                total_extracted += 1
                processed_urls.add(unify_url_id(url))
            except KeyboardInterrupt:
                break

            except Exception as e:
                all_purpose_logger.error(
                    f"Failed to process {domain_record.url} with {e}"
                )
    all_purpose_logger.info(f"Extracted {total_extracted} urls")
    return processed_urls


async def _extract_task(
    domain_record: DomainRecord,
    additional_info: Dict[str, Any],
    pipeline: ProcessorPipeline,
):
    result = []
    try:
        result = await pipeline.process_domain_record(domain_record, additional_info)
    except KeyboardInterrupt as e:
        raise e
    except Exception as e:
        metadata_logger.error(
            f"Failed to process {domain_record.url} with {e}",
            extra={"domain_record": domain_record},
            exc_info=True if metadata_logger.level == logging.DEBUG else False,
        )
    return result


async def extract(
    records: List[Tuple[DomainRecord, Dict[str, Any]]],
    pipeline: ProcessorPipeline,
    concurrent_length: int = 5,
):
    """
    Extracts the records using the pipeline, with at most `concurrent_length`
    records being processed at the same time.

    Args:
        records (List[Tuple[DomainRecord, Dict[str, Any]]]): List of records to process and additional info
        pipeline (ProcessorPipeline): Pipeline to use
        concurrent_length (int, optional): Number of concurrent records to process.
            Defaults to 5.

    """
    domain_records_iterator = iter(tqdm(records))
    domains_exausted = False
    total_extracted: int = 0
    if hasattr(pipeline.downloader, "__aenter__"):
        await pipeline.downloader.__aenter__()  # type: ignore
    try:
        queue: Set[asyncio.Task[List[str]]] = set()
        while not domains_exausted or len(queue) > 0:
            # Put into queue till possible
            while len(queue) < concurrent_length and not domains_exausted:
                next_record = next(domain_records_iterator, None)
                if next_record is None:
                    domains_exausted = True
                    break
                next_domain_record, additional_info = next_record

                queue.add(
                    asyncio.create_task(
                        _extract_task(next_domain_record, additional_info, pipeline)
                    )
                )

            done, queue = await asyncio.wait(queue, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    total_extracted += len(await task)
                except KeyboardInterrupt:
                    break

                except Exception:
                    all_purpose_logger.error(f"Error in task {task}", exc_info=True)
    except Exception as e:
        all_purpose_logger.error(e, exc_info=True)

    finally:
        if hasattr(pipeline.downloader, "__aexit__"):
            await pipeline.downloader.__aexit__(None, None, None)  # type: ignore
    all_purpose_logger.info(f"Extracted {total_extracted} urls")
