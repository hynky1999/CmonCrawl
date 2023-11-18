# import textwrap
# import unittest
# from dataclasses import dataclass
# from datetime import datetime
# from typing import Awaitable, Callable, Iterator, List, TypeVar
# from unittest.mock import patch

# import aioboto3
# import aiobotocore
# import aiobotocore.endpoint
# import boto3
# import botocore
# from moto import mock_athena, mock_s3

# from cmoncrawl.aggregator.athena_query import (
#     QUERIES_SUBFOLDER,
#     AthenaAggregator,
#     DomainRecord,
#     MatchType,
# )
# from cmoncrawl.aggregator.utils.athena_query_maker import (
#     crawl_query,
#     date_to_sql_format,
#     prepare_athena_sql_query,
#     url_query_based_on_match_type,
#     url_query_date_range,
# )
# from tests.utils import MySQLRecordsDB

# T = TypeVar("T")
# R = TypeVar("R")


# @dataclass
# class _PatchedAWSReponseContent:
#     """Patched version of `botocore.awsrequest.AWSResponse.content`"""

#     content: bytes | Awaitable[bytes]

#     def __await__(self) -> Iterator[bytes]:
#         async def _generate_async() -> bytes:
#             if isinstance(self.content, Awaitable):
#                 return await self.content
#             else:
#                 return self.content

#         return _generate_async().__await__()

#     def decode(self, encoding: str) -> str:
#         assert isinstance(self.content, bytes)
#         return self.content.decode(encoding)


# @dataclass
# class _AsyncReader:
#     content: bytes

#     async def read(self, amt: int = -1) -> bytes:
#         ret_val = self.content
#         self.content = b""
#         return ret_val


# @dataclass
# class PatchedAWSResponseRaw:
#     content: _AsyncReader
#     url: str


# class PatchedAWSResponse:
#     """Patched version of `botocore.awsrequest.AWSResponse`"""

#     def __init__(self, response: botocore.awsrequest.AWSResponse) -> None:
#         self._response = response
#         self.status_code = response.status_code
#         self.content = _PatchedAWSReponseContent(response.content)
#         self.raw = PatchedAWSResponseRaw(
#             content=_AsyncReader(response.content), url=response.url
#         )
#         if not hasattr(self.raw, "raw_headers"):
#             self.raw.raw_headers = {}


# def _factory(
#     original: Callable[[botocore.awsrequest.AWSResponse, T], Awaitable[R]]
# ) -> Callable[[botocore.awsrequest.AWSResponse, T], Awaitable[R]]:
#     async def patched_convert_to_response_dict(
#         http_response: botocore.awsrequest.AWSResponse, operation_model: T
#     ) -> R:
#         return await original(PatchedAWSResponse(http_response), operation_model)  # type: ignore[arg-type]

#     return patched_convert_to_response_dict


# class TestAthenaQueryCreation(unittest.IsolatedAsyncioTestCase):
#     async def asyncSetUp(self) -> None:
#         self.CC_SERVERS = [
#             "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
#             "https://index.commoncrawl.org/CC-MAIN-2021-09-index",
#             "https://index.commoncrawl.org/CC-MAIN-2020-50-index",
#         ]
#         self.old_convert_to_response_dict = (
#             aiobotocore.endpoint.convert_to_response_dict
#         )
#         aiobotocore.endpoint.convert_to_response_dict = _factory(
#             aiobotocore.endpoint.convert_to_response_dict
#         )  # type: ignore[assignment]

#     async def asyncTearDown(self) -> None:
#         aiobotocore.endpoint.convert_to_response_dict = (
#             self.old_convert_to_response_dict
#         )

#     def test_prepare_athena_sql_query_multiple_urls(self):
#         query = prepare_athena_sql_query(
#             ["seznam.cz", "idnes.cz"],
#             datetime(2019, 1, 1),
#             datetime(2023, 12, 31),
#             self.CC_SERVERS,
#             match_type=MatchType.EXACT,
#             database="commoncrawl",
#             table="ccindex",
#         )
#         self.assertEqual(
#             query,
#             textwrap.dedent(
#                 """\
#             SELECT cc.url,
#                     cc.fetch_time,
#                     cc.warc_filename,
#                     cc.warc_record_offset,
#                     cc.warc_record_length
#             FROM "commoncrawl"."ccindex" AS cc
#             WHERE (cc.fetch_time BETWEEN CAST('2019-01-01 00:00:00' AS TIMESTAMP) AND CAST('2023-12-31 00:00:00' AS TIMESTAMP)) AND (cc.crawl = 'CC-MAIN-2022-05' OR cc.crawl = 'CC-MAIN-2021-09' OR cc.crawl = 'CC-MAIN-2020-50') AND (cc.fetch_status = 200) AND (cc.subset = 'warc') AND ((cc.url = 'seznam.cz') OR (cc.url = 'idnes.cz'))
#             ORDER BY url;"""
#             ),
#         )

#     def test_prefix_match_type(self):
#         url = "arxiv.org/abs/1905.00075"
#         for prefix in ["http://", "https://", "https://www.", "", "www."]:
#             prefixed_url = f"{prefix}{url}"
#             expected_sql = "(cc.url_host_name = 'arxiv.org' OR cc.url_host_name = 'www.arxiv.org') AND (cc.url_path = '/abs/1905.00075' OR cc.url_path LIKE '/abs/1905.00075/%')"
#             self.assertEqual(
#                 url_query_based_on_match_type(MatchType.PREFIX, prefixed_url),
#                 expected_sql,
#             )

#     def test_host_match_type(self):
#         url = "arxiv.org/abs/1905.00075"
#         for prefix in ["http://", "https://", "https://www.", "", "www."]:
#             prefixed_url = f"{prefix}{url}"
#             expected_sql = (
#                 "cc.url_host_name = 'arxiv.org' OR cc.url_host_name = 'www.arxiv.org'"
#             )
#             self.assertEqual(
#                 url_query_based_on_match_type(MatchType.HOST, prefixed_url),
#                 expected_sql,
#             )

#     def test_domain_match_type(self):
#         url = "arxiv.org/abs/1905.00075"
#         for prefix in ["http://", "https://", "https://www.", "", "www."]:
#             prefixed_url = f"{prefix}{url}"
#             expected_sql = (
#                 "cc.url_host_name LIKE '%.arxiv.org' OR cc.url_host_name = 'arxiv.org'"
#             )
#             self.assertEqual(
#                 url_query_based_on_match_type(MatchType.DOMAIN, prefixed_url),
#                 expected_sql,
#             )

#     def test_exact_match_type(self):
#         url = "www.arxiv.org/abs/1905.00075"
#         expected_sql = "cc.url = 'www.arxiv.org/abs/1905.00075'"
#         for prefix in ["http://", "https://", "https://www.", "", "www."]:
#             prefixed_url = f"{prefix}{url}"
#             expected_sql = f"cc.url = '{prefixed_url}'"
#             self.assertEqual(
#                 url_query_based_on_match_type(MatchType.EXACT, prefixed_url),
#                 expected_sql,
#             )

#     def test_date_range_query(self):
#         self.assertEqual(url_query_date_range(None, None), "")

#     def test_both_dates_present(self):
#         since = datetime(2020, 1, 1)
#         to = datetime(2020, 12, 31)
#         expected = f"cc.fetch_time BETWEEN CAST('{date_to_sql_format(since)}' AS TIMESTAMP) AND CAST('{date_to_sql_format(to)}' AS TIMESTAMP)"
#         self.assertEqual(url_query_date_range(since, to), expected)

#     def test_only_to_date_present(self):
#         to = datetime(2020, 12, 31)
#         expected = f"cc.fetch_time <= CAST('{date_to_sql_format(to)}' AS TIMESTAMP)"
#         self.assertEqual(url_query_date_range(None, to), expected)

#     def test_only_since_date_present(self):
#         since = datetime(2020, 1, 1)
#         expected = f"cc.fetch_time >= CAST('{date_to_sql_format(since)}' AS TIMESTAMP)"
#         self.assertEqual(url_query_date_range(since, None), expected)

#     def test_crawl_query_no_date_filter(self):
#         expected = "cc.crawl = 'CC-MAIN-2022-05' OR cc.crawl = 'CC-MAIN-2021-09' OR cc.crawl = 'CC-MAIN-2020-50'"
#         self.assertEqual(crawl_query(self.CC_SERVERS, None, None), expected)

#     def test_crawl_query_with_since_date_filter(self):
#         # The crawl query only filters based on YEAR
#         since = datetime(2021, 12, 12)
#         expected = "cc.crawl = 'CC-MAIN-2022-05' OR cc.crawl = 'CC-MAIN-2021-09'"
#         self.assertEqual(crawl_query(self.CC_SERVERS, since, None), expected)

#     def test_crawl_query_with_to_date_filter(self):
#         to = datetime(2021, 12, 31)
#         expected = "cc.crawl = 'CC-MAIN-2021-09' OR cc.crawl = 'CC-MAIN-2020-50'"
#         self.assertEqual(crawl_query(self.CC_SERVERS, None, to), expected)

#     def test_crawl_query_with_since_and_to_date_filter(self):
#         since = datetime(2021, 1, 1)
#         to = datetime(2021, 12, 31)
#         expected = "cc.crawl = 'CC-MAIN-2021-09'"
#         self.assertEqual(crawl_query(self.CC_SERVERS, since, to), expected)


# class TestAthenaAggregator(unittest.IsolatedAsyncioTestCase):
#     def setUp(self) -> None:
#         self.mock_s3 = mock_s3()
#         self.mock_s3.start()
#         self.mock_athena = mock_athena()
#         self.mock_athena.start()
#         self.old_convert_to_response_dict = (
#             aiobotocore.endpoint.convert_to_response_dict
#         )
#         aiobotocore.endpoint.convert_to_response_dict = _factory(
#             aiobotocore.endpoint.convert_to_response_dict
#         )  # type: ignore[assignment]

#     def tearDown(self) -> None:
#         self.mock_s3.stop()
#         self.mock_athena.stop()
#         aiobotocore.endpoint.convert_to_response_dict = (
#             self.old_convert_to_response_dict
#         )

#     async def test_athena_aggregator_lifecycle_existing_bucket(self):
#         expected_CC_indexes = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
#         async with aioboto3.Session().client("s3") as s3_client:
#             # Create a bucket called test-bucket
#             await s3_client.create_bucket(Bucket="test-bucket")
#         with patch(
#             "cmoncrawl.aggregator.athena_query.get_all_CC_indexes"
#         ) as mock_get_all_CC_indexes, patch(
#             "cmoncrawl.aggregator.athena_query.commoncrawl_database_and_table_exists"
#         ) as mock_commoncrawl_database_and_table_exists:
#             mock_commoncrawl_database_and_table_exists.return_value = False
#             mock_get_all_CC_indexes.return_value = expected_CC_indexes
#             domains = ["test.com"]
#             aggregator = AthenaAggregator(domains, bucket_name="test-bucket")
#             # Create a bucket called test-bucket
#             await aggregator.aopen()

#         self.assertEqual(aggregator.cc_servers, expected_CC_indexes)
#         # Check that Athena table was created. BotoMock does not support metadata queries so we just check that the queries
#         # were sent to Athena
#         async with aggregator.aws_client.client("athena") as athena_client:
#             queries = await athena_client.list_query_executions()
#             # We should have one query for db creation, table creation and one for repair
#             self.assertEqual(len(queries["QueryExecutionIds"]), 3)

#         # Check that the s3 bucket is cleaned up
#         async with aggregator.aws_client.client("s3") as s3_client:
#             response = await s3_client.list_objects(Bucket="test-bucket")
#             # No objects should be present in the bucket
#             self.assertNotIn("Contents", response)

#         # Add arbitrary object to the bucket
#         async with aggregator.aws_client.client("s3") as s3_client:
#             await s3_client.put_object(Bucket="test-bucket", Key="test-key")

#         # Close the aggregator
#         await aggregator.aclose()

#         # Check that the s3 bucket is still there and was not deleted
#         async with aggregator.aws_client.client("s3") as s3_client:
#             response = await s3_client.list_buckets()
#             bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
#             self.assertIn("test-bucket", bucket_names)

#             # Check that "test-key" is still in the "test-bucket"
#             objects = await s3_client.list_objects(Bucket="test-bucket")
#             object_keys = [obj["Key"] for obj in objects.get("Contents", [])]
#             self.assertIn("test-key", object_keys)

#     async def test_athena_aggregator_lifecycle_new_bucket(self):
#         expected_CC_indexes = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
#         with patch(
#             "cmoncrawl.aggregator.athena_query.get_all_CC_indexes"
#         ) as mock_get_all_CC_indexes, patch(
#             "cmoncrawl.aggregator.athena_query.commoncrawl_database_and_table_exists"
#         ) as mock_commoncrawl_database_and_table_exists:
#             mock_commoncrawl_database_and_table_exists.return_value = False
#             mock_get_all_CC_indexes.return_value = expected_CC_indexes
#             domains = ["test.com"]
#             aggregator = AthenaAggregator(domains)
#             # Create a bucket called test-bucket
#             await aggregator.aopen()

#         self.assertEqual(aggregator.cc_servers, expected_CC_indexes)

#         # Get bucket name
#         bucket_name = aggregator.bucket_name

#         # Check that s3 bucket was created
#         async with aggregator.aws_client.client("s3") as s3_client:
#             response = await s3_client.list_buckets()
#             self.assertEqual(response["Buckets"][0]["Name"], bucket_name)

#         # Check that Athena table was created. BotoMock does not support metadata queries so we just check that the queries
#         # were sent to Athena
#         async with aggregator.aws_client.client("athena") as athena_client:
#             queries = await athena_client.list_query_executions()
#             # We should have one query for db creation, table creation and one for repair
#             self.assertEqual(len(queries["QueryExecutionIds"]), 3)

#         # Check that the s3 bucket is cleaned up
#         async with aggregator.aws_client.client("s3") as s3_client:
#             response = await s3_client.list_objects(Bucket=bucket_name)
#             # No objects should be present in the bucket
#             self.assertNotIn("Contents", response)

#         # Add arbitrary object to the bucket
#         async with aggregator.aws_client.client("s3") as s3_client:
#             await s3_client.put_object(Bucket=bucket_name, Key="test-key")

#         # Close the aggregator
#         await aggregator.aclose()

#         # Check that bucket was deleted
#         async with aioboto3.Session().client("s3") as s3_client:
#             response = await s3_client.list_buckets()
#             bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
#             self.assertNotIn(bucket_name, bucket_names)


# class TestAthenaAggregatorIterator(unittest.IsolatedAsyncioTestCase, MySQLRecordsDB):
#     bucket_name = "test-bucket"

#     async def mocked_await_athena_query(self, query: str, result_name: str) -> str:
#         query = query.replace('"commoncrawl"."ccindex"', "ccindex")
#         query = query.replace("TIMESTAMP", "DATETIME")
#         with self.managed_cursor() as cursor:
#             cursor.execute(query)
#             results = cursor.fetchall()
#             results = [list(row) for row in results]
#             header = [col[0] for col in cursor.description]

#             # get index of fetch_time column
#             fetch_time_index = header.index("fetch_time")

#             # convert datetime to string
#             for row in results:
#                 row[fetch_time_index] = row[fetch_time_index].strftime(
#                     "%Y-%m-%d %H:%M:%S.%f"
#                 )
#         expected_result_key = f"{QUERIES_SUBFOLDER}/{result_name}.csv"

#         # Create a boto3 client
#         s3 = boto3.client("s3")

#         # Convert results to CSV format
#         header_results = [header] + results
#         csv_results = "\n".join([",".join(map(str, row)) for row in header_results])

#         # Write CSV results to S3
#         s3.put_object(
#             Body=csv_results, Bucket=self.bucket_name, Key=expected_result_key
#         )
#         return expected_result_key

#     def setUp(self) -> None:
#         self.mock_s3 = mock_s3()
#         self.mock_s3.start()
#         self.mock_athena = mock_athena()
#         self.mock_athena.start()
#         self.aws_client = aioboto3.Session()
#         # patching
#         self.mock_athena_query = patch(
#             "cmoncrawl.aggregator.athena_query.AthenaAggregator.AthenaAggregatorIterator.await_athena_query"
#         )
#         self.mock_await_athena_query = self.mock_athena_query.start()
#         self.mock_await_athena_query.side_effect = self.mocked_await_athena_query
#         self.old_convert_to_response_dict = (
#             aiobotocore.endpoint.convert_to_response_dict
#         )
#         aiobotocore.endpoint.convert_to_response_dict = _factory(
#             aiobotocore.endpoint.convert_to_response_dict
#         )  # type: ignore[assignment]

#         # db
#         MySQLRecordsDB.setUp(self)

#     async def asyncSetUp(self):
#         # Create a bucket called test-bucket
#         async with self.aws_client.client("s3") as s3_client:
#             await s3_client.create_bucket(Bucket=self.bucket_name)

#     def tearDown(self) -> None:
#         self.mock_s3.stop()
#         self.mock_athena.stop()
#         self.mock_athena_query.stop()
#         MySQLRecordsDB.tearDown(self)
#         aiobotocore.endpoint.convert_to_response_dict = (
#             self.old_convert_to_response_dict
#         )

#     async def test_limit(self):
#         self.domains = ["seznam.cz"]
#         self.iterator = AthenaAggregator.AthenaAggregatorIterator(
#             aws_client=self.aws_client,
#             domains=self.domains,
#             CC_files=["https://index.commoncrawl.org/CC-MAIN-2022-05-index"],
#             since=None,
#             to=None,
#             limit=1,
#             batch_size=1,
#             match_type=MatchType.EXACT,
#             extra_sql_where_clause=None,
#             database_name="commoncrawl",
#             table_name="ccindex",
#             bucket_name=self.bucket_name,
#         )
#         records: List[DomainRecord] = []
#         async for record in self.iterator:
#             records.append(record)
#         self.assertEqual(len(records), 1)

#     async def test_since(self):
#         self.domains = ["seznam.cz"]
#         since = datetime(2022, 1, 2)
#         self.iterator = AthenaAggregator.AthenaAggregatorIterator(
#             aws_client=self.aws_client,
#             domains=self.domains,
#             CC_files=[
#                 "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2021-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2023-09-index",
#             ],
#             since=since,
#             to=None,
#             limit=None,
#             batch_size=1,
#             match_type=MatchType.EXACT,
#             extra_sql_where_clause=None,
#             database_name="commoncrawl",
#             table_name="ccindex",
#             bucket_name=self.bucket_name,
#         )
#         records: List[DomainRecord] = []
#         async for record in self.iterator:
#             records.append(record)

#         self.assertEqual(len(records), 3)
#         for record in records:
#             self.assertGreaterEqual(record.timestamp, since)

#     async def test_to(self):
#         self.domains = ["seznam.cz"]
#         to = datetime(2022, 1, 2)
#         self.iterator = AthenaAggregator.AthenaAggregatorIterator(
#             aws_client=self.aws_client,
#             domains=self.domains,
#             CC_files=[
#                 "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2021-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2023-09-index",
#             ],
#             since=None,
#             to=to,
#             limit=None,
#             batch_size=1,
#             match_type=MatchType.EXACT,
#             extra_sql_where_clause=None,
#             database_name="commoncrawl",
#             table_name="ccindex",
#             bucket_name=self.bucket_name,
#         )
#         records: List[DomainRecord] = []
#         async for record in self.iterator:
#             records.append(record)

#         self.assertEqual(len(records), 3)
#         for record in records:
#             self.assertLessEqual(record.timestamp, to)

#     async def test_batch_size_single(self):
#         self.domains = ["seznam.cz"]
#         self.iterator = AthenaAggregator.AthenaAggregatorIterator(
#             aws_client=self.aws_client,
#             domains=self.domains,
#             CC_files=[
#                 "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2021-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2023-09-index",
#             ],
#             since=None,
#             to=None,
#             limit=None,
#             batch_size=1,
#             match_type=MatchType.EXACT,
#             extra_sql_where_clause=None,
#             database_name="commoncrawl",
#             table_name="ccindex",
#             bucket_name=self.bucket_name,
#         )
#         records: List[DomainRecord] = []
#         async for record in self.iterator:
#             records.append(record)

#         self.assertEqual(len(records), 5)
#         # Check that the mocked Athena query was called for each CC file
#         self.assertEqual(self.mock_await_athena_query.call_count, 3)

#     async def test_batch_size_zero(self):
#         self.domains = ["seznam.cz"]
#         self.iterator = AthenaAggregator.AthenaAggregatorIterator(
#             aws_client=self.aws_client,
#             domains=self.domains,
#             CC_files=[
#                 "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2021-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2023-09-index",
#             ],
#             since=None,
#             to=None,
#             limit=None,
#             batch_size=0,
#             match_type=MatchType.EXACT,
#             extra_sql_where_clause=None,
#             database_name="commoncrawl",
#             table_name="ccindex",
#             bucket_name=self.bucket_name,
#         )
#         records: List[DomainRecord] = []
#         async for record in self.iterator:
#             records.append(record)

#         self.assertEqual(len(records), 5)
#         # Check that the mocked Athena query was called just once
#         self.assertEqual(self.mock_await_athena_query.call_count, 1)

#     async def test_extra_sql_where(self):
#         self.domains = ["seznam.cz"]
#         where_clause = 'cc.warc_filename = "filename1"'
#         self.iterator = AthenaAggregator.AthenaAggregatorIterator(
#             aws_client=self.aws_client,
#             domains=self.domains,
#             CC_files=[
#                 "https://index.commoncrawl.org/CC-MAIN-2022-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2021-05-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2023-09-index",
#             ],
#             since=None,
#             to=None,
#             limit=None,
#             batch_size=0,
#             match_type=MatchType.EXACT,
#             extra_sql_where_clause=where_clause,
#             database_name="commoncrawl",
#             table_name="ccindex",
#             bucket_name=self.bucket_name,
#         )
#         records: List[DomainRecord] = []
#         async for record in self.iterator:
#             records.append(record)

#         self.assertEqual(len(records), 1)
