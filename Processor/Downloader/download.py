from __future__ import annotations
import asyncio
from base64 import b32decode
import gzip
import logging
import random
from types import TracebackType
from aiohttp import ClientError, ClientSession
from typing import Type
from hashlib import md5, sha1

from Downloader.errors import PageDownloadException
from Downloader.warc import PipeMetadata, parse_warc
from utils import DomainRecord


class Downloader:
    def __init__(
        self,
        base_url: str = "https://data.commoncrawl.org/",
        digest_verification: bool = True,
        max_retries: int = 3,
        sleep_step: int = 1,
    ):
        self.digest_verification = digest_verification
        self.BASE_URL = base_url
        self.max_retries = max_retries
        self.__sleep_step = sleep_step

    async def aopen(self) -> Downloader:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()
        return self

    async def __aenter__(self) -> Downloader:
        return await self.aopen()

    async def download(
        self, domain_record: DomainRecord, metadata: PipeMetadata, retries: int = 0
    ) -> str:
        # Because someone thought having non c-like range is good idea
        # Both end/start are inclusive
        headers = {
            "Range": "bytes={}-{}".format(
                domain_record.offset, domain_record.offset + domain_record.length - 1
            )
        }
        url = f"{self.BASE_URL}{domain_record.filename}"
        try:
            response_bytes = bytes()
            async with self.client.get(url, headers=headers) as response:
                if not response.ok:
                    raise PageDownloadException(
                        domain_record, response.reason, response.status
                    )
                # will be unziped
                response_bytes = await response.content.read()
            content = parse_warc(
                self.unwrap(response_bytes, metadata.encoding),
                metadata,
            )
        except (ClientError, TimeoutError) as e:
            logging.error(e)
            if retries < self.max_retries:
                await asyncio.sleep(
                    random.randint(0, (retries + 1) * self.__sleep_step)
                )
                return await self.download(domain_record, metadata, retries + 1)
            else:
                raise ValueError(f"Failed to download {domain_record.url}")

        if self.digest_verification:
            hash_type: str
            hash: str
            if metadata.warc_header is None:
                raise ValueError("No digest found")

            digest = metadata.warc_header.get("payload_digest")
            if digest is None:
                logging.warn(
                    "Warc header has no digest, using digest from domain record"
                )
                if metadata.domain_record.digest is None:
                    raise ValueError("Digest is missing in domain record")
                hash_type, hash = "sha1", metadata.domain_record.digest
            else:
                hash_type, hash = digest.split(":")
            digest = metadata.domain_record.digest
            if digest is not None and digest != hash:
                raise ValueError(f'Digest mismatch: "{digest}" != "{hash}"')

            if self.verify_digest(hash_type, hash, content, metadata.encoding) == False:
                raise ValueError("Digest verification failed")

            return content

    def verify_digest(
        self, hash_type: str, digest: str, content: str, encoding: str
    ) -> bool:
        # ADD md5
        digest_decoded = b32decode(digest)
        hash_digest = ""
        if hash_type == "sha1":
            hash_digest = sha1(content.encode(encoding)).digest()

        elif hash_type == "md5":
            hash_digest = md5(content.encode(encoding)).digest()
        else:
            raise ValueError(f"Unknown hash type {hash_type}")

        if digest_decoded != hash_digest:
            logging.warn("")
            return False
        return True

    def unwrap(self, response: bytes, encoding: str) -> str:
        content = gzip.decompress(response).decode(encoding)

        return content

    async def aclose(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> Downloader:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> Downloader:
        return await self.aclose(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
