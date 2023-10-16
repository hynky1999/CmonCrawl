from __future__ import annotations
import asyncio
from collections import deque
from datetime import datetime
from pathlib import Path
import tempfile
from typing import Any, AsyncIterable, AsyncIterator, Deque, List, Optional, Set, Tuple
import uuid
import hashlib

import aiofiles
from aiocsv.readers import AsyncDictReader
from cmoncrawl.aggregator.utils.athena_query_maker import (
    crawl_url_to_name,
    prepare_athena_sql_query,
)
from cmoncrawl.aggregator.utils.helpers import get_all_CC_indexes
from cmoncrawl.common.loggers import all_purpose_logger

from aiohttp import ClientSession
from cmoncrawl.aggregator.index_query import crawl_to_year

import aioboto3

from cmoncrawl.common.types import (
    DomainCrawl,
    DomainRecord,
    MatchType,
    RetrieveResponse,
)

QUERIES_SUBFOLDER = "queries"
QUERIES_TMP_SUBFOLDER = "queries_tmp"


async def run_athena_query(
    session: aioboto3.Session, query_kwargs: dict[str, Any]
) -> str:
    async with session.client(
        "athena",
        region_name=session.region_name
        if session.region_name is not None
        else "us-east-1",
    ) as athena:
        query_result = await athena.start_query_execution(
            **query_kwargs,
        )
        if query_result["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(f"Athena query failed: {query_result}")

        query_execution_id = query_result["QueryExecutionId"]
        while True:
            response = await athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response["QueryExecution"]["Status"]["State"]
            if status in ["FAILED", "CANCELLED"]:
                all_purpose_logger.error(f"Athena query failed: {response}")
                raise Exception(f"Athena query failed: {response}")

            if status == "SUCCEEDED":
                break

            await asyncio.sleep(5)
    return query_execution_id


class AthenaAggregator(AsyncIterable[DomainRecord]):
    def __init__(
        self,
        domains: List[str],
        cc_indexes_server: str = "http://index.commoncrawl.org/collinfo.json",
        match_type: MatchType = MatchType.EXACT,
        cc_servers: List[str] = [],
        since: datetime = datetime.min,
        to: datetime = datetime.max,
        limit: int | None = None,
        prefetch_size: int = 3,
        max_retry: int = 5,
        extra_sql_where_clause: str | None = None,
        aws_profile: str = "default",
        bucket_name: Optional[str] = None,
        catalog_name: str = "AwsDataCatalog",
        database_name: str = "commoncrawl",
        table_name: str = "ccindex",
    ) -> None:
        self.domains = domains
        self.cc_indexes_server = cc_indexes_server
        self.match_type = match_type
        self.cc_servers = cc_servers
        self.since = since
        self.to = to
        self.limit = limit
        self.max_retry = max_retry
        self.aws_profile = aws_profile
        self.extra_sql_where_clause = extra_sql_where_clause
        self.prefetch_size = prefetch_size

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

    async def __aenter__(self) -> AthenaAggregator:
        return await self.aopen()

    async def __aexit__(self, exc_type, exc_value, traceback) -> AthenaAggregator:
        return await self.aclose()

    async def aopen(self) -> AthenaAggregator:
        self.aws_client = aioboto3.Session(
            profile_name=self.aws_profile, region_name="us-east-1"
        )
        async with ClientSession() as client:
            if len(self.cc_servers) == 0:
                self.cc_servers = await get_all_CC_indexes(
                    client, self.cc_indexes_server
                )
        # create bucket if not exists
        async with self.aws_client.client("s3") as s3:
            # Check if bucket exists
            bucket_response = await s3.head_bucket(Bucket=self.bucket_name)
            if bucket_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                await s3.create_bucket(Bucket=self.bucket_name)
        # create database and table if not exists
        if not await commoncrawl_database_and_table_exists(
            self.aws_client, self.catalog_name, self.database_name, self.table_name
        ):
            await create_commoncrawl_database_and_table(
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
            # Delete the s3 bucket
            paginator = s3.get_paginator("list_objects_v2")
            async for result in paginator.paginate(Bucket=self.bucket_name):
                for file in result["Contents"]:
                    await s3.delete_object(Bucket=self.bucket_name, Key=file["Key"])

            await s3.delete_bucket(Bucket=self.bucket_name)

    class AthenaAggregatorIterator(AsyncIterator[DomainRecord]):
        def __init__(
            self,
            aws_client: aioboto3.Session,
            domains: List[str],
            CC_files: List[str],
            match_type: MatchType,
            since: Optional[datetime],
            to: Optional[datetime],
            limit: int | None,
            extra_sql_where_clause: str | None,
            bucket_name: str,
            database_name: str,
            table_name: str,
        ):
            self.__aws_client = aws_client
            self.__since = since
            self.__to = to
            self.__crawls_remaining: List[str] = self.init_crawls_queue(CC_files)
            self.__domain_records: Deque[DomainRecord] = deque()
            self.__limit = limit
            self.__match_type = match_type
            self.__domains = domains
            self.__total = 0
            self.__bucket_name = bucket_name
            self.__database_name = database_name
            self.__table_name = table_name
            self.__extra_sql_where_clause = extra_sql_where_clause
            self.__prefetch_queue: Set[asyncio.Task[List[DomainRecord]]] = set()
            self.__opt_prefetch_size = 5

        def init_crawls_queue(self, CC_files: List[str]) -> List[str]:
            allowed_crawls = [
                crawl_url_to_name(crawl)
                for crawl in CC_files
                if (self.__since is None or crawl_to_year(crawl) >= self.__since.year)
                and (self.__to is None or crawl_to_year(crawl) <= self.__to.year)
            ]
            return allowed_crawls

        async def download_results(self, key: str) -> str:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                async with self.__aws_client.client("s3") as s3:
                    await s3.download_file(self.__bucket_name, key, temp_file.name)

            return temp_file.name

        async def await_athena_query(self, query: str, result_name: str) -> str:
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
            csv_file = await self.download_results(csv_file)
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

        # TODO Tenacity for retries
        async def __fetch_next_crawl(self, crawl: str):
            query = prepare_athena_sql_query(
                self.__domains,
                self.__since,
                self.__to,
                [crawl],
                self.__database_name,
                self.__table_name,
                self.__match_type,
                self.__extra_sql_where_clause,
            )
            query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
            query_id = f"{crawl}-{query_hash}"
            crawl_s3_key = await self.is_crawl_cached(query_id)
            if crawl_s3_key is not None:
                all_purpose_logger.info(f"Using cached crawl {crawl}")
            else:
                all_purpose_logger.info(f"Querying for crawl {crawl}")
                crawl_s3_key = await self.await_athena_query(query, query_id)

            domain_records: List[DomainRecord] = []
            async for domain_record in self.domain_records_from_s3(crawl_s3_key):
                domain_records.append(domain_record)
            return domain_records

        async def __await_next_prefetch(self):
            """
            Gets the next athen result and adds it to the prefetch queue
            """
            # Wait for the next prefetch to finish
            # Don't prefetch if limit is set to avoid overfetching
            while len(self.__crawls_remaining) > 0 and (
                len(self.__prefetch_queue) == 0
                or (
                    self.__limit is None
                    and len(self.__prefetch_queue) <= self.__opt_prefetch_size
                )
            ):
                next_crawl = self.__crawls_remaining.pop(0)
                self.__prefetch_queue.add(
                    asyncio.create_task(self.__fetch_next_crawl(next_crawl))
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
                        all_purpose_logger.error(f"Error during a crawl query", e)

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
        return AthenaAggregator.AthenaAggregatorIterator(
            self.aws_client,
            self.domains,
            self.cc_servers,
            self.match_type,
            self.since,
            self.to,
            self.limit,
            self.extra_sql_where_clause,
            self.bucket_name,
            self.database_name,
            self.table_name,
        )


async def commoncrawl_database_and_table_exists(
    session: aioboto3.Session, catalog_name: str, database_name: str, table_name: str
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


async def create_commoncrawl_database_and_table(
    session: aioboto3.Session, s3_bucket: str, catalog: str, database: str, table: str
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
        async with session.client("s3") as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for result in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
                for file in result["Contents"]:
                    await s3.delete_object(Bucket=s3_bucket, Key=file["Key"])
