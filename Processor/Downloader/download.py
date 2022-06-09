from __future__ import annotations
from base64 import b32decode
import gzip
from types import TracebackType
from aiohttp import ClientSession
from typing import Tuple, Type
from Aggregator.index_query import DomainRecord
from hashlib import sha1

from Processor.Downloader.errors import PageDownloadException
from Processor.Downloader.warc import PipeMetadata, parse_warc


DEFAULT_ENCODE = "latin-1"


class Downloader:
    def __init__(
        self,
        base_url: str = "https://data.commoncrawl.org/",
        digest_verification: bool = True,
    ):
        self.digest_verification = digest_verification
        self.BASE_URL = base_url

    async def aopen(self) -> Downloader:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()
        return self

    async def __aenter__(self) -> Downloader:
        return await self.aopen()

    async def download(
        self, domain_record: DomainRecord, digest: str = ""
    ) -> Tuple[str, PipeMetadata]:
        # Because someone thought having non c-like range is good idea
        # Both end/start are inclusive
        headers = {
            "Range": "bytes={}-{}".format(
                domain_record.offset, domain_record.offset + domain_record.length - 1
            )
        }
        url = f"{self.BASE_URL}{domain_record.filename}"

        async with self.client.get(url, headers=headers) as response:
            if not response.ok:
                raise PageDownloadException(
                    domain_record, response.reason, response.status
                )
            # will be unziped
            reponse_bytes = await response.content.read()
            content, metadata = parse_warc(self.unwrap(reponse_bytes))

            if digest_verification:
                hash_type: str
                hash: str
                hash_type, hash = metadata.warc_header["payload_digest"].split(":")
                if digest != hash:
                    raise ValueError(f'Digest mismatch: "{digest}" != "{hash}"')

                if self.verify_digest(hash_type, hash, content) == False:
                    raise ValueError("Digest verification failed")

            return content, metadata

    def verify_digest(
        self, hash_type: str, digest: str, content: str, encoding: str = DEFAULT_ENCODE
    ) -> bool:
        # ADD md5
        digest_decoded = b32decode(digest)
        if hash_type == "sha1":
            hash_digest = sha1(content.encode(encoding)).digest()
            if digest_decoded != hash_digest:
                return False
        else:
            raise ValueError(f"Unknown hash type {hash_type}")
        return True

    def unwrap(self, response: bytes) -> str:
        content = gzip.decompress(response).decode(DEFAULT_ENCODE)
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
