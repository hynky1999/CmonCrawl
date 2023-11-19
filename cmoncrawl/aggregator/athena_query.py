from __future__ import annotations

import asyncio
import hashlib
import logging
import tempfile
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import (
    AsyncIterator,
    Deque,
    List,
    Optional,
    Set,
)

import aioboto3
import aiofiles
import tenacity
from aiocsv.readers import AsyncDictReader
from aiohttp import ClientSession

from cmoncrawl.aggregator.base import IAggregator
from cmoncrawl.aggregator.gateway_query import crawl_to_year
from cmoncrawl.aggregator.utils.athena_query_maker import (
    crawl_url_to_name,
    prepare_athena_sql_query,
)
from cmoncrawl.aggregator.utils.constants import CC_INDEXES_SERVER
from cmoncrawl.aggregator.utils.helpers import (
    get_all_CC_indexes,
    remove_bucket_prefix,
    run_athena_query,
)
from cmoncrawl.common.loggers import all_purpose_logger
from cmoncrawl.common.types import (
    DomainRecord,
    MatchType,
)

QUERIES_SUBFOLDER = "queries"
QUERIES_TMP_SUBFOLDER = "queries_tmp"


class AthenaAggregator(IAggregator):
    """
    This class is responsible for aggregating the index files from commoncrawl using AWS Athena.
    It is an async context manager which can then be used as an async iterator
    which yields DomainRecord objects, found in the index files of commoncrawl.

    It uses the AWS Athena to query from s3 the index files of commoncrawl.

    Args:
        urls (List[str]): A list of urls to search for.
        cc_indexes_server (str, optional): The commoncrawl index server to use. Defaults to "http://index.commoncrawl.org/collinfo.json".
        match_type (MatchType, optional): Match type for cdx-api. Defaults to MatchType.EXACT.
        cc_servers (List[str], optional): A list of commoncrawl servers to use. If None, then indexes will be retrieved from the cc_indexes_server. Defaults to None.
        since (datetime, optional): The start date for the search. Defaults to datetime.min.
        to (datetime, optional): The end date for the search. Defaults to datetime.max.
        limit (int, optional): The maximum number of results to return. Defaults to None.
        prefetch_size (int, optional): The number of indexes to fetch concurrently. Defaults to 3.
        max_retry (int, optional): The maximum number of retries for a single request. Defaults to 5.
        extra_sql_where_clause (str, optional): Additional SQL WHERE clause to append to the Athena query. Defaults to None.
        batch_size (int): How many crawls to query at once. Defaults to 1. If <= 0, all crawls will be queried at once.
        aws_profile (str, optional): The AWS profile to use for Athena and S3. Defaults to "default".
        bucket_name (str, optional): The S3 bucket to use for Athena query results. If None, a new bucket will be created. Defaults to None.
        catalog_name (str, optional): The Athena catalog to use. Defaults to "AwsDataCatalog".
        database_name (str, optional): The Athena database to use. Defaults to "commoncrawl".
        table_name (str, optional): The Athena table to use. Defaults to "ccindex".

    Examples:
        >>> async with AthenaAggregator(["example.com"]) as aggregator:
        >>>     async for domain_record in aggregator:
        >>>         print(domain_record)

    """

    def __init__(
        self,
        urls: List[str],
        match_type: MatchType = MatchType.EXACT,
        cc_servers: Optional[List[str]] = None,
        since: datetime = datetime.min,
        to: datetime = datetime.max,
        limit: int | None = None,
        prefetch_size: int = 2,
        sleep_base: float = 1.3,
        max_retry: int = 5,
        extra_sql_where_clause: str | None = None,
        batch_size: int = 1,
        aws_profile: Optional[str] = None,
        bucket_name: Optional[str] = None,
        catalog_name: str = "AwsDataCatalog",
        database_name: str = "commoncrawl",
        table_name: str = "ccindex",
    ) -> None:
        self.urls = urls
        self.match_type = match_type
        self.cc_servers = cc_servers
        self.since = since
        self.to = to
        self.limit = limit
        self.max_retry = max_retry
        self.extra_sql_where_clause = extra_sql_where_clause
        self.prefetch_size = prefetch_size
        self.batch_size = batch_size
        self.sleep_base = sleep_base

        # AWS
        self.aws_profile = aws_profile
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.table_name = table_name
        # Only delete bucket if we created it
        self.delete_bucket = bucket_name is None

        if bucket_name is None:
            self.bucket_name = f"commoncrawl-{uuid.uuid4()}"
            all_purpose_logger.info(f"Using bucket {self.bucket_name}")
        else:
            self.bucket_name = bucket_name

        self.__validate_args()

    def __validate_args(self):
        if self.limit and self.batch_size <= 0:
            raise ValueError(
                "If you want to limit the number of results, batch_size must be > 0, to avoid overfetching"
            )

    async def __aenter__(self) -> AthenaAggregator:
        return await self.aopen()

    async def __aexit__(self, exc_type, exc_value, traceback) -> AthenaAggregator:
        return await self.aclose()

    async def aopen(self) -> AthenaAggregator:
        self.aws_client = aioboto3.Session(
            profile_name=self.aws_profile, region_name="us-east-1"
        )
        async with ClientSession() as client:
            if not self.cc_servers:
                self.cc_servers = await get_all_CC_indexes(client, CC_INDEXES_SERVER)
        # create bucket if not exists
        async with self.aws_client.client("s3") as s3:
            # Check if bucket exists
            bucket = None
            try:
                bucket = await s3.head_bucket(Bucket=self.bucket_name)
            except s3.exceptions.ClientError:
                pass

            if bucket is None:
                await s3.create_bucket(Bucket=self.bucket_name)
                all_purpose_logger.info(f"Created bucket {self.bucket_name}")
            else:
                all_purpose_logger.info(f"Using bucket {self.bucket_name}")

        # create database and table if not exists
        if not await self.__commoncrawl_database_and_table_exists(
            self.aws_client,
            self.catalog_name,
            self.database_name,
            self.table_name,
        ):
            await self.__create_commoncrawl_database_and_table(
                self.aws_client,
                self.bucket_name,
                self.catalog_name,
                self.database_name,
                self.table_name,
            )
        return self

    async def aclose(self) -> AthenaAggregator:
        await self.cleanup()
        return self

    async def cleanup(self):
        if not self.delete_bucket:
            return

        all_purpose_logger.info(f"Deleting bucket {self.bucket_name}")
        async with self.aws_client.client("s3") as s3:
            await remove_bucket_prefix(self.aws_client, self.bucket_name, "")
            await s3.delete_bucket(Bucket=self.bucket_name)

    async def __commoncrawl_database_and_table_exists(
        self,
        session: aioboto3.Session,
        catalog_name: str,
        database_name: str,
        table_name: str,
    ):
        async with session.client("athena") as athena:
            try:
                response = await athena.list_table_metadata(
                    CatalogName=catalog_name,
                    DatabaseName=database_name,
                    Expression=table_name,
                )
            except athena.exceptions.MetadataException:
                return False
            return len(response["TableMetadataList"]) > 0

    async def __create_commoncrawl_database_and_table(
        self,
        session: aioboto3.Session,
        s3_bucket: str,
        catalog: str,
        database: str,
        table: str,
    ):
        prefix = f"DDL-{uuid.uuid4()}"
        results_location = f"s3://{s3_bucket}/{prefix}"
        try:
            create_database_query = f"""
                CREATE DATABASE IF NOT EXISTS {database}
            """
            await run_athena_query(
                session,
                {
                    "QueryString": create_database_query,
                    "QueryExecutionContext": {"Catalog": catalog},
                    "ResultConfiguration": {"OutputLocation": results_location},
                },
            )

            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
            url_surtkey                   STRING,
            url                           STRING,
            url_host_name                 STRING,
            url_host_tld                  STRING,
            url_host_2nd_last_part        STRING,
            url_host_3rd_last_part        STRING,
            url_host_4th_last_part        STRING,
            url_host_5th_last_part        STRING,
            url_host_registry_suffix      STRING,
            url_host_registered_domain    STRING,
            url_host_private_suffix       STRING,
            url_host_private_domain       STRING,
            url_protocol                  STRING,
            url_port                      INT,
            url_path                      STRING,
            url_query                     STRING,
            fetch_time                    TIMESTAMP,
            fetch_status                  SMALLINT,
            content_digest                STRING,
            content_mime_type             STRING,
            content_mime_detected         STRING,
            content_charset               STRING,
            content_languages             STRING,
            warc_filename                 STRING,
            warc_record_offset            INT,
            warc_record_length            INT,
            warc_segment                  STRING)
            PARTITIONED BY (
            crawl                         STRING,
            subset                        STRING)
            STORED AS parquet
            LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';
            """
            await run_athena_query(
                session,
                {
                    "QueryString": create_table_query,
                    "QueryExecutionContext": {"Database": database},
                    "ResultConfiguration": {"OutputLocation": results_location},
                },
            )
            # TODO make sure that if table is not partitioned, this function will run
            repair_statement = f"MSCK REPAIR TABLE {database}.{table};"
            await run_athena_query(
                session,
                {
                    "QueryString": repair_statement,
                    "QueryExecutionContext": {"Database": database},
                    "ResultConfiguration": {"OutputLocation": results_location},
                },
            )
        finally:
            # remove all query results
            await remove_bucket_prefix(session, s3_bucket, prefix)

    class AthenaAggregatorIterator(AsyncIterator[DomainRecord]):
        def __init__(
            self,
            aws_client: aioboto3.Session,
            urls: List[str],
            cc_servers: List[str],
            match_type: MatchType,
            since: Optional[datetime],
            to: Optional[datetime],
            limit: int | None,
            prefetch_size: int,
            sleep_base: float,
            max_retry: int,
            batch_size: int,
            extra_sql_where_clause: str | None,
            bucket_name: str,
            database_name: str,
            table_name: str,
        ):
            self.__aws_client = aws_client
            self.__since = since
            self.__to = to
            self.__crawls_remaining: List[List[str]] = self.init_crawls_queue(
                cc_servers, batch_size
            )
            self.__domain_records: Deque[DomainRecord] = deque()
            self.__limit = limit
            self.__match_type = match_type
            self.__urls = urls
            self.__total = 0
            self.__bucket_name = bucket_name
            self.__database_name = database_name
            self.__table_name = table_name
            self.__extra_sql_where_clause = extra_sql_where_clause
            self.__prefetch_queue: Set[asyncio.Task[List[DomainRecord]]] = set()
            self.__opt_prefetch_size = prefetch_size
            self.__sleep_base = sleep_base
            self.__max_retry = max_retry

        def init_crawls_queue(
            self, CC_files: List[str], batch_size: int
        ) -> List[List[str]]:
            allowed_crawls = [
                crawl_url_to_name(crawl)
                for crawl in CC_files
                if (self.__since is None or crawl_to_year(crawl) >= self.__since.year)
                and (self.__to is None or crawl_to_year(crawl) <= self.__to.year)
            ]
            if batch_size <= 0:
                return [allowed_crawls]
            return [
                allowed_crawls[i : i + batch_size]
                for i in range(0, len(allowed_crawls), batch_size)
            ]

        async def __download_results(self, key: str) -> str:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                async with self.__aws_client.client("s3") as s3:
                    await s3.download_file(self.__bucket_name, key, temp_file.name)

            return temp_file.name

        async def __await_athena_query(self, query: str, result_name: str) -> str:
            s3_location = f"s3://{self.__bucket_name}/{QUERIES_TMP_SUBFOLDER}"
            query_execution_id = await run_athena_query(
                self.__aws_client,
                {
                    "QueryString": query,
                    "QueryExecutionContext": {"Database": self.__database_name},
                    "ResultConfiguration": {"OutputLocation": s3_location},
                },
            )
            # Move file to bucket/result_name
            query_result_key = f"{QUERIES_TMP_SUBFOLDER}/{query_execution_id}.csv"
            expected_result_key = f"{QUERIES_SUBFOLDER}/{result_name}.csv"
            async with self.__aws_client.client("s3") as s3:
                await s3.copy_object(
                    Bucket=self.__bucket_name,
                    CopySource=f"{self.__bucket_name}/{query_result_key}",
                    Key=expected_result_key,
                )
                await s3.delete_object(
                    Bucket=self.__bucket_name,
                    Key=f"{QUERIES_TMP_SUBFOLDER}/{query_execution_id}.csv",
                )
            return expected_result_key

        async def domain_records_from_s3(
            self, csv_file: str
        ) -> AsyncIterator[DomainRecord]:
            # download file
            csv_file = await self.__download_results(csv_file)
            try:
                async with aiofiles.open(csv_file, mode="r") as afp:
                    async for row in AsyncDictReader(afp):
                        yield DomainRecord(
                            url=row["url"],
                            timestamp=datetime.strptime(
                                row["fetch_time"], "%Y-%m-%d %H:%M:%S.%f"
                            ),
                            filename=row["warc_filename"],
                            offset=int(row["warc_record_offset"]),
                            length=int(row["warc_record_length"]),
                        )
            finally:
                # remove file
                Path(csv_file).unlink()

        async def is_crawl_cached(self, query_id: str):
            key = f"{QUERIES_SUBFOLDER}/{query_id}.csv"
            async with self.__aws_client.client("s3") as s3:
                try:
                    await s3.head_object(Bucket=self.__bucket_name, Key=key)
                    return key
                except s3.exceptions.ClientError:
                    return None

        async def __fetch_next_crawl_batch(self, crawl_batch: List[str]):
            query = prepare_athena_sql_query(
                self.__urls,
                self.__since,
                self.__to,
                crawl_batch,
                self.__database_name,
                self.__table_name,
                self.__match_type,
                self.__extra_sql_where_clause,
            )
            query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
            crawl_batch_id = (
                crawl_batch[0]
                if len(crawl_batch) == 1
                else f"{crawl_batch[0]}...{crawl_batch[-1]}"
            )
            query_id = f"{crawl_batch_id}-{query_hash}"
            crawl_s3_key = await self.is_crawl_cached(query_id)
            if crawl_s3_key is not None:
                all_purpose_logger.info(f"Using cached crawl batch {crawl_batch}")
            else:
                all_purpose_logger.info(f"Querying for crawl batch {crawl_batch}")
                crawl_s3_key = await self.__await_athena_query(query, query_id)

            domain_records: List[DomainRecord] = []
            async for domain_record in self.domain_records_from_s3(crawl_s3_key):
                domain_records.append(domain_record)
            return domain_records

        async def __await_next_prefetch(self):
            """
            Gets the next athen result and adds it to the prefetch queue
            """

            @tenacity.retry(
                wait=tenacity.wait_random_exponential(
                    multiplier=5, exp_base=self.__sleep_base, max=120
                ),
                retry=tenacity.retry_if_exception_type(ValueError),
                stop=tenacity.stop_after_attempt(self.__max_retry),
                before_sleep=tenacity.before_sleep_log(
                    all_purpose_logger, logging.INFO
                ),
                reraise=True,
            )
            async def fetch_next_crawl_batch(crawl_batch: List[str]):
                return await self.__fetch_next_crawl_batch(crawl_batch)

            # Wait for the next prefetch to finish
            # Don't prefetch if limit is set to avoid overfetching
            while len(self.__crawls_remaining) > 0 and (
                len(self.__prefetch_queue) == 0
                or (
                    self.__limit is None
                    and len(self.__prefetch_queue) <= self.__opt_prefetch_size
                )
            ):
                next_crawl_batch = self.__crawls_remaining.pop(0)
                self.__prefetch_queue.add(
                    asyncio.create_task(fetch_next_crawl_batch(next_crawl_batch))
                )

            while len(self.__prefetch_queue) > 0 and len(self.__domain_records) == 0:
                done, self.__prefetch_queue = await asyncio.wait(
                    self.__prefetch_queue, return_when="FIRST_COMPLETED"
                )
                for task in done:
                    try:
                        domain_records = task.result()
                        self.__domain_records.extend(domain_records)
                    except Exception as e:
                        all_purpose_logger.error(f"Error during a crawl query {str(e)}")

        async def __anext__(self) -> DomainRecord:
            # Stop if we fetched everything or reached limit
            if self.__limit is not None and self.__total >= self.__limit:
                raise StopAsyncIteration

            # Nothing prefetched
            while len(self.__domain_records) == 0:
                await self.__await_next_prefetch()
                if (
                    len(self.__prefetch_queue) == 0
                    and len(self.__crawls_remaining) == 0
                    and len(self.__domain_records) == 0
                ):
                    # No more data to fetch
                    raise StopAsyncIteration

            result = self.__domain_records.popleft()
            self.__total += 1
            return result

    def __aiter__(self) -> AsyncIterator[DomainRecord]:
        if not self.cc_servers:
            raise ValueError("cc_servers must be initialized before iterating")
        return AthenaAggregator.AthenaAggregatorIterator(
            aws_client=self.aws_client,
            urls=self.urls,
            cc_servers=self.cc_servers,
            match_type=self.match_type,
            since=self.since,
            to=self.to,
            limit=self.limit,
            prefetch_size=self.prefetch_size,
            sleep_base=self.sleep_base,
            max_retry=self.max_retry,
            batch_size=self.batch_size,
            extra_sql_where_clause=self.extra_sql_where_clause,
            bucket_name=self.bucket_name,
            database_name=self.database_name,
            table_name=self.table_name,
        )
