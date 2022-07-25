from datetime import datetime
from pathlib import Path
import re
from typing import List

import bs4
from Processor.App.Downloader.downloader import Downloader
from Processor.App.processor_utils import DomainRecord, PipeMetadata, all_purpose_logger
from aiofiles import open as asyncOpen


class DownloaderDummy(Downloader):
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
