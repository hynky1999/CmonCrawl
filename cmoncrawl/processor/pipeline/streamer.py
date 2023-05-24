from abc import ABC, abstractmethod
import asyncio
import json
from pathlib import Path
import random
from typing import Any, Dict, List
from xmlrpc.client import Boolean
from cmoncrawl.common.types import (
    PipeMetadata,
)
from cmoncrawl.common.loggers import all_purpose_logger, metadata_logger
from aiofiles import open as asyncOpen

import os

"""
All classes defined here should only modify how are the data streamed out.
It shouldn't choose which data to stream out, this should be handled by
the extractors.
"""


class IStreamer(ABC):
    """
    Base class for all outstreamers
    """

    def __init__(self):
        pass

    @abstractmethod
    async def stream(
        self, extracted_data: Dict[Any, Any], metadata: PipeMetadata
    ) -> Path:
        raise NotImplementedError()

    @abstractmethod
    async def clean_up(self) -> None:
        raise NotImplementedError()


class StreamerDummy(IStreamer):
    """
    Dummy Streamer which keeps the output is memory
    """

    def __init__(self):
        self.data: Dict[str, Dict[Any, Any]] = {}

    async def stream(self, extracted_data: Dict[Any, Any], metadata: PipeMetadata):
        self.data[metadata.domain_record.filename] = extracted_data
        return Path("/")

    async def clean_up(self):
        pass


class BaseStreamerFile(IStreamer, ABC):
    """
    Abstract Class which defines the basic functionality of a file streamer
    """

    def __init__(
        self,
        root: Path,
        max_directory_size: int,
        max_file_size: int,
        extension: str,
        directory_prefix: str = "directory_",
        max_retries: int = 3,
    ):
        # To create new folder
        self.directory_size = max_directory_size
        self.max_directory_size = max_directory_size
        self.file_size = 0
        self.max_file_size = max_file_size
        self.root = root
        self.diretory_prefix = directory_prefix
        self.directories: List[Path] = []
        self.extension = extension
        self.max_retries = max_retries

    def __get_folder_path(self) -> Path:
        return self.root / Path(f"{self.diretory_prefix}{len(self.directories)-1}")

    def __get_new_folder_path(self) -> Path:
        return self.root / Path(f"{self.diretory_prefix}{len(self.directories)}")

    def __create_new_folder(self, folder_path: Path):
        os.makedirs(folder_path, exist_ok=True)
        self.directory_size = len(os.listdir(folder_path))
        self.directories.append(folder_path)
        all_purpose_logger.debug(
            f"Created new folder {folder_path} with capacity {self.directory_size}/{self.max_directory_size}"
        )

    async def clean_up(self) -> None:
        try:
            for directory in self.directories:
                for root, dirs, files in os.walk(directory, topdown=False):
                    for name in files:
                        all_purpose_logger.debug(f"Removing {root}/{name}")
                        os.remove(Path(root) / name)
                    for name in dirs:
                        all_purpose_logger.debug(f"Removing {root}/{name}")
                        os.rmdir(Path(root) / name)
                all_purpose_logger.debug(f"Removing {directory}")
                os.rmdir(directory)
            all_purpose_logger.debug(f"Removing {self.root}")
            os.rmdir(self.root)
        except OSError as e:
            all_purpose_logger.error(f"{e}")

    def get_file_name(self, metadata: PipeMetadata):
        name = "file"
        if self.max_file_size == 1:
            name = metadata.name or metadata.url_parsed.hostname or name
        return f"{self.directory_size}_{name}{self.extension}"

    @abstractmethod
    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        raise NotImplementedError

    async def stream(
        self, extracted_data: Dict[Any, Any], metadata: PipeMetadata
    ) -> Path:
        # Preemptive so we dont' have to lock
        if self.file_size >= self.max_file_size:
            self.file_size = 1
            self.directory_size += 1
        else:
            self.file_size += 1

        while self.directory_size >= self.max_directory_size:
            all_purpose_logger.debug(
                f"Reached capacity of folder {self.__get_folder_path()} {self.directory_size}/{self.max_directory_size}"
            )
            self.__create_new_folder(self.__get_new_folder_path())

        directory_size = self.directory_size

        file_path = Path(self.__get_folder_path()) / self.get_file_name(metadata)
        path = await self.__stream(file_path, extracted_data, metadata, 0)
        metadata_logger.debug(
            f"Wrote {file_path} {directory_size}/{self.max_directory_size}",
            extra={"domain_record": metadata.domain_record},
        )
        return path

    async def __stream(
        self,
        file_path: Path,
        extracted_data: Dict[Any, Any],
        metadata: PipeMetadata,
        retries: int,
    ) -> Path:
        if retries > self.max_retries:
            raise OSError(f"Failed to write to {file_path}")
        metadata_logger.debug(
            f"Writing to {file_path}", extra={"domain_record": metadata.domain_record}
        )
        try:
            async with asyncOpen(file_path, "a") as f:
                out = self.metadata_to_string(extracted_data) + "\n"
                await f.write(out)
        except OSError as e:
            metadata_logger.error(
                f"{e}\n retrying {retries}/{self.max_retries}",
                extra={"domain_record": metadata.domain_record},
            )
            await asyncio.sleep(random.randint(1, 2))
            return await self.__stream(file_path, extracted_data, metadata, retries + 1)
        return file_path


class StreamerFileJSON(BaseStreamerFile):
    def __init__(
        self,
        root: Path,
        max_directory_size: int,
        max_file_size: int,
        pretty: Boolean = False,
    ):
        super().__init__(root, max_directory_size, max_file_size, extension=".jsonl")
        self.pretty = pretty

    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        indent = None
        separator = (",", ":")
        if self.pretty:
            indent = 4
            separator = (",", ": ")

        return json.dumps(
            extracted_data,
            sort_keys=True,
            default=str,
            ensure_ascii=False,
            indent=indent,
            separators=separator,
        )


class StreamerFileHTML(BaseStreamerFile):
    def __init__(
        self,
        root: Path,
        max_directory_size: int,
    ):
        super().__init__(
            root,
            extension=".html",
            max_directory_size=max_directory_size,
            max_file_size=1,
        )

    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        return extracted_data["html"]
