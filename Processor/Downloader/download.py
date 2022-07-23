from __future__ import annotations
import asyncio
from base64 import b32decode
import io
import random
from tabnanny import check
from types import TracebackType
from aiohttp import ClientError, ClientSession
from typing import List, Tuple, Type
from hashlib import md5, sha1

from Downloader.errors import PageDownloadException
from utils import DomainRecord, PipeMetadata, metadata_logger
from warcio import ArchiveIterator

ALLOWED_ERR_FOR_RETRIES = [500, 502, 503, 504]


class Downloader:
    def __init__(
        self,
        base_url: str = "https://data.commoncrawl.org/",
        digest_verification: bool = True,
        max_retry: int = 5,
        sleep_step: int = 3,
    ):
        self.digest_verification = digest_verification
        self.BASE_URL = base_url
        self.max_retry = max_retry
        self.__sleep_step = sleep_step

    async def aopen(self) -> Downloader:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()
        return self

    async def __aenter__(self) -> Downloader:
        return await self.aopen()

    async def download(self, domain_record: DomainRecord):
        # Because someone thought having non c-like range is good idea
        # Both end/start are inclusive
        headers = {
            "Range": "bytes={}-{}".format(
                domain_record.offset, domain_record.offset + domain_record.length - 1
            )
        }
        url = f"{self.BASE_URL}{domain_record.filename}"
        for retry in range(self.max_retry):
            try:
                response_bytes = bytes()
                async with self.client.get(url, headers=headers) as response:
                    if not response.ok:
                        raise PageDownloadException(
                            domain_record, response.reason, response.status
                        )
                    # will be unziped, we cannot use the stream since warcio doesn't support async
                    response_bytes = await response.content.read()
                    return self.unwrap(response_bytes, domain_record)
            except (
                ClientError,
                TimeoutError,
                PageDownloadException,
            ) as e:
                max_retry = self.max_retry
                if (
                    isinstance(e, PageDownloadException)
                    and e.status not in ALLOWED_ERR_FOR_RETRIES
                ):
                    max_retry = retry

                metadata_logger.error(
                    f"Failed to retrieve from domain_record retry: {retry}/{max_retry}",
                    extra={"domain_record": domain_record},
                )
                if retry >= self.max_retry:
                    break
                await asyncio.sleep(random.randint(0, (retry + 1) * self.__sleep_step))

        return []

    def unwrap(
        self, response: bytes, domain_record: DomainRecord
    ) -> List[Tuple[str, PipeMetadata]]:
        ariter = ArchiveIterator(
            io.BytesIO(response), check_digests="raise", arc2warc=True
        )
        encoding = "latin-1"
        warcs = [
            (
                warc.content_stream().read().decode(encoding),
                PipeMetadata(
                    domain_record,
                    warc_header=dict(warc.rec_headers.headers),
                    http_header=dict(warc.http_headers.headers),
                    encoding=encoding,
                ),
            )
            for warc in ariter
        ]
        return warcs

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
