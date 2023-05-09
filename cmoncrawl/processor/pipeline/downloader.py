from __future__ import annotations
import asyncio
from datetime import datetime
import io
from pathlib import Path
import random
import re
from types import TracebackType
from aiohttp import ClientError, ClientSession
from typing import List, Tuple, Type
from aiofiles import open as asyncOpen


import bs4

from cmoncrawl.common.types import (
    DomainRecord,
    PipeMetadata,
)
from cmoncrawl.common.loggers import metadata_logger, all_purpose_logger
from warcio import ArchiveIterator

ALLOWED_ERR_FOR_RETRIES = [500, 502, 503, 504]


class IDownloader:
    async def download(
        self, domain_record: DomainRecord
    ) -> (List[Tuple[str, PipeMetadata]]):
        raise NotImplementedError()


class AsyncDownloader(IDownloader):
    def __init__(
        self,
        base_url: str = "https://data.commoncrawl.org/",
        digest_verification: bool = True,
        max_retry: int = 5,
        sleep_step: int = 10,
        encoding: str = "latin-1",
    ):
        self.digest_verification = digest_verification
        self.BASE_URL = base_url
        self.__max_retry = max_retry
        self.__sleep_step = sleep_step
        self.encoding = encoding

    async def aopen(self) -> AsyncDownloader:
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()
        return self

    async def __aenter__(self) -> AsyncDownloader:
        return await self.aopen()

    async def download(self, domain_record: DomainRecord):
        def should_retry(retry: int, reason: str, status: int, **args: str):
            metadata_logger.error(
                f"Failed to retrieve from domain_record {status}: {reason} retry: {retry+1}/{self.__max_retry} add_info: {args}",
                extra={"domain_record": domain_record},
            )
            if status not in ALLOWED_ERR_FOR_RETRIES:
                return False

            return True

        # Because someone thought having non c-like range is good idea
        # Both end/start are inclusive
        headers = {
            "Range": "bytes={}-{}".format(
                domain_record.offset, domain_record.offset + domain_record.length - 1
            )
        }
        url = f"{self.BASE_URL}{domain_record.filename}"
        for retry in range(self.__max_retry):
            try:
                response_bytes = bytes()
                async with self.client.get(url, headers=headers) as response:
                    if not response.ok:
                        reason: str = response.reason if response.reason else "Unknown"
                        if not should_retry(retry, reason, response.status):
                            break
                    else:
                        # will be unziped, we cannot use the stream since warcio doesn't support async
                        response_bytes = await response.content.read()
                        return self.unwrap(response_bytes, domain_record)
            except (
                ClientError,
                TimeoutError,
            ) as e:
                if not should_retry(retry, f"{str(e)} {type(e)}", 0):
                    raise e
            await asyncio.sleep(random.randint(0, (retry + 1) * self.__sleep_step))
        ret: List[Tuple[str, PipeMetadata]] = []
        return ret

    def unwrap(
        self, response: bytes, domain_record: DomainRecord
    ) -> List[Tuple[str, PipeMetadata]]:
        ariter = ArchiveIterator(
            io.BytesIO(response), check_digests="raise", arc2warc=True
        )
        encoding = self.encoding
        warcs: List[Tuple[str, PipeMetadata]] = [
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
    ) -> IDownloader:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None = None,
    ) -> IDownloader:
        return await self.aclose(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)


class DownloaderDummy(IDownloader):
    """
    Dummy downloader for testing
    It doesn't download anything but return files passed in the constructor
    and extracts metadata from the file
    """

    def __init__(
        self, files: List[Path], url: str | None = None, date: datetime | None = None
    ):
        self.files = files
        self.file_index = 0
        self.date = date
        self.url = url

    async def download(self, domain_record: DomainRecord):
        if self.file_index >= len(self.files):
            raise IndexError("No more files to download")

        async with asyncOpen(self.files[self.file_index], "r") as f:
            content = await f.read()
        metadata = self.mine_metadata(content, self.files[self.file_index])
        self.file_index += 1
        return [(content, metadata)]

    def mine_metadata(self, content: str, file_path: Path):
        bs4_article = bs4.BeautifulSoup(content, "html.parser")
        url = self.url
        if url is None:
            url = self.extract_url(bs4_article)
        date = self.date
        if date is None:
            date = self.extract_year(file_path)
        return PipeMetadata(
            domain_record=DomainRecord(
                filename=file_path.name,
                url=url,
                offset=0,
                length=len(content),
                timestamp=date,
            ),
            encoding="utf-8",
        )

    def extract_year(self, file_path: Path):
        year_re = re.search(r"\d{4}", file_path.name)
        if year_re is None:
            date = datetime(2020, 1, 1)
        else:
            date = datetime(int(year_re.group(0)), 1, 1)
        return date

    def extract_url(self, content: bs4.BeautifulSoup):
        url = None
        # Extract
        url_match = content.select_one("meta[property='og:url']")
        # This is terrible but works
        if url is None:
            if url_match:
                url = url_match.get("content")
        if url is None:
            url_match = content.select_one("link[rel='home']")
            if url_match:
                url = url_match.get("href")
        if url is None:
            url_match = content.select_one("link[title*='RSS']")
            if url_match:
                url = url_match.get("href")
        if url is None:
            url_match = content.select_one("link[media*='handheld']")
            if url_match:
                url = url_match.get("href")
        if url is None:
            raise ValueError("No url found")

        if isinstance(url, list):
            url = url[0]
        all_purpose_logger.debug(f"Found url: {url}")
        return url
