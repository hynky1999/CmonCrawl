import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Awaitable, Callable, Iterator, TypeVar

import aiobotocore
import aiobotocore.endpoint
import botocore
import MySQLdb
from dotenv import load_dotenv


class MySQLRecordsDB(unittest.TestCase):
    @contextmanager
    def managed_cursor(self):
        cursor = self.db.cursor()
        cursor.execute(f"USE {self.db_name}")
        try:
            yield cursor
        finally:
            cursor.close()

    def seed_db(self):
        with self.managed_cursor() as cursor:
            cursor = self.db.cursor()
            records = [
                # Seznam
                [
                    "seznam.cz",
                    "2021-01-03 00:00:00.000",
                    "filename3",
                    100,
                    200,
                    "CC-MAIN-2021-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2022-01-01 00:00:00.000",
                    "filename1",
                    100,
                    200,
                    "CC-MAIN-2022-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2022-01-02 00:00:00.000",
                    "filename2",
                    100,
                    200,
                    "CC-MAIN-2022-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2022-01-03 00:00:00.000",
                    "filename3",
                    100,
                    200,
                    "CC-MAIN-2022-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2023-01-03 00:00:00.000",
                    "filename3",
                    100,
                    200,
                    "CC-MAIN-2023-09",
                    200,
                    "warc",
                ],
            ]
            values = ", ".join(
                [
                    f"('{record[0]}', '{record[1]}', '{record[2]}', {record[3]}, {record[4]}, '{record[5]}', {record[6]}, '{record[7]}')"
                    for record in records
                ]
            )
            insert_data_sql = f"""
                INSERT INTO {self.table_name} (url, fetch_time, warc_filename, warc_record_offset, warc_record_length, crawl, fetch_status, subset)
                VALUES {values}
            """
            cursor.execute(insert_data_sql)
            self.db.commit()

    def remove_db(self):
        cursor = self.db.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
        self.db.commit()
        cursor.close()

    def setUp(self):
        # Connect to MySQL
        load_dotenv()
        self.db = MySQLdb.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            passwd=os.getenv("MYSQL_PASSWORD"),
            port=int(os.getenv("MYSQL_PORT")),
        )
        self.db_name = os.getenv("MYSQL_DB_NAME")
        self.table_name = os.getenv("MYSQL_TABLE_NAME")

        # Create a Cursor object to execute SQL commands
        self.remove_db()
        cursor = self.db.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        cursor.close()
        self.db.commit()

        with self.managed_cursor() as cursor:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    url VARCHAR(255),
                    fetch_time TIMESTAMP,
                    warc_filename VARCHAR(255),
                    warc_record_offset INT,
                    warc_record_length INT,
                    crawl VARCHAR(50),
                    fetch_status INT,
                    subset VARCHAR(50)
                )
            """
            cursor.execute(create_table_sql)
            self.db.commit()

        self.seed_db()

    def tearDown(self):
        self.remove_db()
        self.db.close()


# Moto3 for aioboto3
T = TypeVar("T")
R = TypeVar("R")


@dataclass
class _PatchedAWSReponseContent:
    """Patched version of `botocore.awsrequest.AWSResponse.content`"""

    content: bytes | Awaitable[bytes]

    def __await__(self) -> Iterator[bytes]:
        async def _generate_async() -> bytes:
            if isinstance(self.content, Awaitable):
                return await self.content
            else:
                return self.content

        return _generate_async().__await__()

    def decode(self, encoding: str) -> str:
        assert isinstance(self.content, bytes)
        return self.content.decode(encoding)


@dataclass
class _AsyncReader:
    content: bytes

    async def read(self, amt: int = -1) -> bytes:
        ret_val = self.content
        self.content = b""
        return ret_val


@dataclass
class PatchedAWSResponseRaw:
    content: _AsyncReader
    url: str


class PatchedAWSResponse:
    """Patched version of `botocore.awsrequest.AWSResponse`"""

    def __init__(self, response: botocore.awsrequest.AWSResponse) -> None:
        self._response = response
        self.status_code = response.status_code
        self.content = _PatchedAWSReponseContent(response.content)
        self.raw = PatchedAWSResponseRaw(
            content=_AsyncReader(response.content), url=response.url
        )
        if not hasattr(self.raw, "raw_headers"):
            self.raw.raw_headers = {}


def _factory(
    original: Callable[[botocore.awsrequest.AWSResponse, T], Awaitable[R]]
) -> Callable[[botocore.awsrequest.AWSResponse, T], Awaitable[R]]:
    async def patched_convert_to_response_dict(
        http_response: botocore.awsrequest.AWSResponse, operation_model: T
    ) -> R:
        return await original(PatchedAWSResponse(http_response), operation_model)  # type: ignore[arg-type]

    return patched_convert_to_response_dict


class MotoMock(unittest.TestCase):
    mocked_envs = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
        "AWS_DEFAULT_REGION",
    ]

    def setUp(self) -> None:
        self.old_environ = {env: os.environ.get(env, None) for env in self.mocked_envs}

        for env in self.mocked_envs:
            os.environ[env] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        self.old_convert_to_response_dict = (
            aiobotocore.endpoint.convert_to_response_dict
        )
        aiobotocore.endpoint.convert_to_response_dict = _factory(
            aiobotocore.endpoint.convert_to_response_dict
        )  # type: ignore[assignment]

    def tearDown(self) -> None:
        for env in self.mocked_envs:
            if self.old_environ[env] is None:
                del os.environ[env]
            else:
                os.environ[env] = self.old_environ[env]

        aiobotocore.endpoint.convert_to_response_dict = (
            self.old_convert_to_response_dict
        )  # type: ignore[assignment]
