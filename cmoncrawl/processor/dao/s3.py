from typing import Any

import aioboto3
from botocore.exceptions import NoCredentialsError

from cmoncrawl.processor.dao.base import DownloadError, ICC_Dao
from botocore.config import Config
from botocore.exceptions import ClientError

from cmoncrawl.common.types import DomainRecord


class S3Dao(ICC_Dao):
    """
    S3Dao is a class that provides methods to interact with AWS S3 for downloading warc files from the commoncrawl bucket.

    Args:
        aws_profile (str, optional): The AWS profile to use for the download. Defaults to None.
        bucket_name (str, optional): The name of the S3 bucket. Defaults to "commoncrawl".

    Attributes:
        bucket_name (str): The name of the S3 bucket.
        aws_profile (str): The AWS profile to use for the download.
        client (aioboto3.client): The S3 client.

    Methods:
        __aenter__(): Asynchronous context manager method to initialize the S3 client.
        __aexit__(exc_type, exc, tb): Asynchronous context manager method to clean up the S3 client.
        fetch(domain_record): Downloads a warc file from the commoncrawl bucket using S3 and returns its bytes.

    Raises:
        ValueError: If the S3Dao client is not initialized.

    """

    def __init__(
        self, aws_profile: str | None = None, bucket_name: str = "commoncrawl"
    ) -> None:
        self.bucket_name = bucket_name
        self.aws_profile = aws_profile
        self.client = None

    async def __aenter__(self) -> "S3Dao":
        # We handle the retries ourselves, so we disable the botocore retries
        config = Config(
            retries={
                "max_attempts": 1,
            }
        )
        self.client = (
            await aioboto3.Session(profile_name=self.aws_profile)
            .client("s3", config=config)
            .__aenter__()
        )  # type: ignore
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.client is not None:
            await self.client.__aexit__(exc_type, exc, tb)  # type: ignore

    async def fetch(self, domain_record: DomainRecord) -> bytes:
        """
        Downloads a warc file from commoncrawl bucket using s3 and returns its bytes.

        Args:
            domain_record (DomainRecord): The domain record to use for the download.
            aws_profile (str): The AWS profile to use for the download.

        Returns:
            bytes: The bytes of the downloaded warc file.
        """

        if self.client is None:
            raise ValueError(
                "S3Dao client is not initialized, did you forget to use async with?"
            )

        file_name = domain_record.filename
        byte_range = f"bytes={domain_record.offset}-{domain_record.offset+domain_record.length-1}"

        try:
            response = await self.client.get_object(
                Bucket=self.bucket_name, Key=file_name, Range=byte_range
            )
            file_bytes = await response["Body"].read()
        except ClientError as e:
            raise DownloadError(f"AWS: {e.response['Error']['Message']}", 500)

        return file_bytes
