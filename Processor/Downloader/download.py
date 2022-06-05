from __future__ import annotations
from base64 import b32decode
import gzip
from types import TracebackType
from aiohttp import ClientSession
from typing import Any, Dict, Type
from Aggregator.errors import PageResponseError
from hashlib import sha1


DEFAULT_ENCODE = "latin-1"


class Downloader:
    def __init__(self, base_url: str = "https://data.commoncrawl.org/"):
        self.BASE_URL = base_url

    async def aopen(self) -> Downloader:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()
        return self

    async def __aenter__(self) -> Downloader:
        return await self.aopen()

    async def download_url(
        self,
        cc_url: str,
        offset: int,
        length: int,
        digest_verification: bool = True,
        digest: str = "",
        pipe_params: Dict[str, Any] = {},
    ) -> str:
        # Because someone thought having non c-like range is good idea
        # Both end/start are inclusive
        headers = {"Range": "bytes={}-{}".format(offset, offset + length - 1)}
        url = f"{self.BASE_URL}{cc_url}"

        async with self.client.get(url, headers=headers) as response:
            if not response.ok:
                raise PageResponseError(url, response.reason, response.status)
            # will be unziped
            reponse_bytes = await response.content.read()
            content = self.unwrap(reponse_bytes, pipe_params)
            pipe_params["url"] = url
            content = pipe_params["content"]
            # TODO verification
            if digest_verification:
                hash_type, hash = pipe_params["warc_header"]["payload_digest"].split(
                    ":"
                )
                if digest != hash:
                    raise ValueError(f'Digest mismatch: "{digest}" != "{hash}"')

                if self.verify_digest(hash_type, hash, content) == False:
                    raise ValueError("Digest verification failed")

            return pipe_params["content"]

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

    def unwrap(self, response: bytes, pipe_params: Dict[str, Any] = {}) -> str:
        content = gzip.decompress(response).decode(DEFAULT_ENCODE)
        return pipe_params["content"]

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
