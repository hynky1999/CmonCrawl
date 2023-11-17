from __future__ import annotations

import asyncio
import io
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    IO,
    Any,
    Callable,
    ContextManager,
    Coroutine,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import bs4
from aiofiles import open as asyncOpen
from tenacity import (
    RetryCallState,
    retry,
    retry_any,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)
from tqdm import tqdm
from warcio import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from cmoncrawl.common.loggers import all_purpose_logger, metadata_logger
from cmoncrawl.common.types import DomainRecord, PipeMetadata
from cmoncrawl.processor.connectors.base import ICC_Dao, DownloadError


import time


def log_after_retry(retry_state: RetryCallState):
    retry_num = retry_state.attempt_number - 1
    if all_purpose_logger.level <= logging.DEBUG or retry_num % 5 == 0:
        reason = str(retry_state.outcome.exception())

        log_message = f"Failed to retrieve from domain_record {reason} retry: {retry_num}"
        if retry_state.next_action:
            log_message += (
                f", waiting {round(retry_state.next_action.sleep, 2)} seconds"
            )
        metadata_logger.error(
            log_message,
            extra={"domain_record": retry_state.args[0]},
        )


T = TypeVar("T")


class Throttler:
    def __init__(self, milliseconds: int):
        """
        Initializes the Throttler class.

        Args:
            milliseconds (int): The number of milliseconds to wait between function calls.
        """
        self.milliseconds = milliseconds
        self.last_call = 0
        self.semaphore = asyncio.Semaphore(1)

    async def throttle(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Throttles the function call.

        Precondition:
            func (Callable[..., Coroutine[Any, Any, T]]): The function to throttle. It must be a coroutine function.
            *args (Any): The positional arguments to pass to the function.
            **kwargs (Any): The keyword arguments to pass to the function.

        Postcondition:
            Returns the result of the throttled function call.

        Returns:
            T: The return type of the function.
        """
        async with self.semaphore:
            elapsed = time.time() - self.last_call
            if elapsed < self.milliseconds / 1000:
                await asyncio.sleep((self.milliseconds / 1000) - elapsed)
            self.last_call = time.time()
        return await func(*args, **kwargs)


class IDownloader:
    """
    Base class for all downloaders
    """

    async def download(
        self, domain_record: DomainRecord | None
    ) -> Iterable[Tuple[str, PipeMetadata]]:
        raise NotImplementedError()


class AsyncDownloader(IDownloader):
    """
    Downloader which asynchronously downloads the the data for the domain_record

    Args:
        base_url (str, optional): Base url where to download data from. Defaults to "https://data.commoncrawl.org/".
        digest_verification (bool, optional): Whether to verify the digest of the downloaded data. Defaults to True.
        max_retry (int, optional): Maximum number of retries. Defaults to 5.
        sleep_base (float, optional): Base sleep time for exponential backoff in retries. Defaults to 1.5.
        max_requests_per_second (int, optional): Maximum number of requests per second. Defaults to 30, please be respectful to others
        encoding: Default encoding to be used

    """

    def __init__(
        self,
        dao: ICC_Dao,
        digest_verification: bool = True,
        max_retry: int = 5,
        sleep_base: float = 1.5,
        max_requests_per_second: int = 30,
        encoding: str = "latin-1",
    ):
        if max_requests_per_second > 500:
            logging.warning(
                "You use more than 500 requests per second, the commoncrawl is shared resource, please be considerate"
            )
        self.digest_verification = digest_verification
        self.download_client = dao
        self.__max_retry = max_retry
        self.__sleep_base = sleep_base
        self.throttler = Throttler(int(1000 / max_requests_per_second))
        self.encoding = encoding

    async def download(self, domain_record: DomainRecord | None):
        if domain_record is None:
            raise ValueError(
                "Async downloader needs domain record, to download"
            )

        @retry(
            stop=stop_after_attempt(self.__max_retry + 1),
            wait=wait_random_exponential(exp_base=self.__sleep_base, max=120),
            retry=retry_any(
                retry_if_exception_type((DownloadError)),
            ),
            reraise=True,
            before_sleep=log_after_retry,
        )
        async def download_throttled(domain_record: DomainRecord):
            warc_bytes: bytes = await self.throttler.throttle(
                self.download_client.fetch, domain_record
            )
            return self.unwrap(warc_bytes, domain_record)

        ret: List[Tuple[str, PipeMetadata]] = await download_throttled(
            domain_record
        )
        return ret

    def unwrap(
        self, response: bytes, domain_record: DomainRecord
    ) -> List[Tuple[str, PipeMetadata]]:
        ariter = ArchiveIterator(
            io.BytesIO(response), check_digests="raise", arc2warc=True  # type: ignore wrong typing in package
        )
        encoding = self.encoding
        warcs: List[Tuple[str, PipeMetadata]] = [
            (
                warc.content_stream().read().decode(encoding),
                PipeMetadata(
                    domain_record,
                    warc_header=dict(
                        warc.rec_headers.headers if warc.rec_headers else {}
                    ),
                    http_header=dict(
                        warc.http_headers.headers if warc.rec_headers else {}
                    ),
                    encoding=encoding,
                    rec_type=warc.rec_type,
                ),
            )
            for warc in ariter
        ]
        return warcs


class WarcIterator(IDownloader, ContextManager["WarcIterator"]):
    """
    WarcIterator is local downloader which iterates over the specified warc file

    Args:
        file (Path): Path to the warc file
        encoding (str, optional): Encoding to be used. Defaults to "latin-1".


    """

    def __init__(
        self,
        file: Path,
        encoding: str = "latin-1",
        show_progress: bool = False,
    ):
        self.file = file
        self.encoding = "latin-1"
        self.file_context: Optional[IO[bytes]] = None
        self.show_progress = show_progress

    def __enter__(self) -> WarcIterator:
        self.file_context = open(self.file, "rb")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file_context:
            self.file_context.close()

    def __get_warc_date(self, warc: ArcWarcRecord) -> datetime:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        return datetime.strptime(warc.rec_headers["WARC-Date"], fmt).replace(
            tzinfo=timezone.utc
        )

    async def download(
        self, domain_record: DomainRecord | None
    ) -> Generator[Tuple[str, PipeMetadata], None, None]:
        if not self.file_context:
            raise Exception("Context not initialized")
        ariter = ArchiveIterator(
            self.file_context, check_digests="raise", arc2warc=True  # type: ignore wrong typing in package
        )
        if self.show_progress:
            all_purpose_logger.info(f"Calculating length of {self.file.name}")
            length = sum(
                1
                for _ in ArchiveIterator(
                    self.file_context, check_digests="raise", arc2warc=True  # type: ignore wrong typing in package
                )
            )
            ariter = tqdm(ariter, total=length)
            self.file_context.seek(0)

        encoding = self.encoding
        warcs = (
            (
                warc.content_stream().read().decode(encoding),
                PipeMetadata(
                    DomainRecord(
                        filename=self.file.name,
                        url=warc.rec_headers["WARC-Target-URI"]
                        if warc.rec_headers
                        else "",
                        offset=self.file_context.tell(),
                        length=warc.length,
                        timestamp=self.__get_warc_date(warc)
                        if warc.rec_headers
                        else None,
                    ),
                    warc_header=dict(
                        warc.rec_headers.headers if warc.rec_headers else {}
                    ),
                    http_header=dict(
                        warc.http_headers.headers if warc.http_headers else {}
                    ),
                    encoding=encoding,
                    rec_type=warc.rec_type,
                ),
            )
            for warc in ariter
        )
        return warcs


class DownloaderLocalFiles(IDownloader):
    """
    Local file downloader and metadata extractor for testing
    It doesn't download anything but passes local files further in the pipeline
    and extracts metadata from the file

    Args:
        files (List[Path]): List of local files to pass
        url (str, optional): Url to use for metadata. Defaults to None.
        date (datetime, optional): Date to add to metadata. Defaults to None.
    """

    def __init__(
        self,
        files: List[Path],
        url: str | None = None,
        date: datetime | None = None,
    ):
        self.files = files
        self.file_index = 0
        self.date = date
        self.url = url

    async def download(self, domain_record: DomainRecord | None):
        if self.file_index >= len(self.files):
            raise IndexError("No more files to pass")

        async with asyncOpen(self.files[self.file_index], "r") as f:
            content = await f.read()
        metadata = self.extract_metadata(content, self.files[self.file_index])
        self.file_index += 1
        return [(content, metadata)]

    def extract_metadata(self, content: str, file_path: Path):
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


class DummyDownloader(IDownloader):
    """
    A dummy downloader class that does not perform any actual downloading. It simply adds an empty string as the content
    and passes the domain record further into the pipeline.
    """

    async def download(self, domain_record: DomainRecord | None):
        """
        Downloads the content for the given domain record.

        Args:
            domain_record (DomainRecord | None): The domain record to download.

        Returns:
            List[Tuple[str, PipeMetadata]]: A list containing a single tuple with an empty string as the first element
                and the pipe metadata as the second element.
        """
        if domain_record is None:
            raise ValueError(
                "Dummy downloader needs domain record to pass it further"
            )

        return [
            (
                "",
                PipeMetadata(
                    domain_record=domain_record,
                    encoding="utf-8",
                ),
            )
        ]
