import asyncio
import json
import logging
from pathlib import Path
import random
from typing import Any, Dict, List
from xmlrpc.client import Boolean
from Downloader.warc import PipeMetadata
from OutStreamer.outstreamer import OutStreamer
from aiofiles import open as asyncOpen

import os


class OutStreamerFileDefault(OutStreamer):
    def __init__(
        self,
        origin: Path,
        extension: str = ".out",
        directory_prefix: str = "directory_",
        max_directory_size: int = 500,
        max_retries: int = 3,
    ):
        # To create new folder
        self.directory_size = max_directory_size
        self.origin = origin
        self.max_directory_size = max_directory_size
        self.diretory_prefix = directory_prefix
        self.directories: List[Path] = []
        self.extension = extension
        self.max_retries = max_retries

    def __get_folder_path(self) -> Path:
        return self.origin / Path(f"{self.diretory_prefix}{len(self.directories)-1}")

    def __get_new_folder_path(self) -> Path:
        return self.origin / Path(f"{self.diretory_prefix}{len(self.directories)}")

    def __create_new_folder(self, folder_path: Path):
        os.makedirs(folder_path, exist_ok=True)
        self.directory_size = len(os.listdir(folder_path))
        self.directories.append(folder_path)
        logging.debug(
            f"Create new folder {folder_path} with capacity {self.directory_size}/{self.max_directory_size}"
        )

    async def clean_up(self) -> None:
        try:
            for directory in self.directories:
                for root, dirs, files in os.walk(directory, topdown=False):
                    for name in files:
                        logging.debug(f"Removing {root}/{name}")
                        os.remove(Path(root) / name)
                    for name in dirs:
                        logging.debug(f"Removing {root}/{name}")
                        os.rmdir(Path(root) / name)
                logging.debug(f"Removing {directory}")
                os.rmdir(directory)
            logging.debug(f"Removing {self.origin}")
            os.rmdir(self.origin)
        except OSError as e:
            logging.error(f"{e}")

    def get_file_name(self, metadata: PipeMetadata):
        name = metadata.name
        if name is None:
            name = f"{metadata.url_parsed.hostname}"

        return f"{name}_{self.directory_size}{self.extension}"

    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        output = "\r\n\r\n".join(
            [f"{key}: \r\n {value}" for key, value in extracted_data.items()]
        )
        return output

    async def stream(
        self, extracted_data: Dict[Any, Any], metadata: PipeMetadata
    ) -> Path:
        while self.directory_size >= self.max_directory_size:
            logging.debug(
                f"Reached capacity of folder {self.__get_folder_path()} {self.directory_size}/{self.max_directory_size}"
            )
            self.__create_new_folder(self.__get_new_folder_path())
        # Preemptive so we dont' have to lock
        self.directory_size += 1
        directory_size = self.directory_size

        file_path = Path(self.__get_folder_path()) / self.get_file_name(metadata)
        path = await self.__stream(file_path, extracted_data, 0)
        logging.debug(f"Wrote {file_path} {directory_size}/{self.max_directory_size}")
        return path

    async def __stream(
        self,
        file_path: Path,
        extracted_data: Dict[Any, Any],
        retries: int,
    ) -> Path:
        if retries > self.max_retries:
            raise OSError("Failed to write file", extracted_data)
        logging.debug(f"Writing to {file_path}")
        try:
            async with asyncOpen(file_path, "w") as f:
                out = self.metadata_to_string(extracted_data)
                await f.write(out)
        except OSError as e:
            logging.error(f"{e}\n retrying {retries}/{self.max_retries}")
            await asyncio.sleep(random.randint(1, 2))
            return await self.__stream(file_path, extracted_data, retries + 1)
        return file_path


class OutStreamerFileJSON(OutStreamerFileDefault):
    def __init__(
        self,
        origin: Path,
        extension: str = ".json",
        pretty: Boolean = False,
        **kwargs: Dict[Any, Any],
    ):
        super().__init__(origin, extension, **kwargs)
        self.pretty = pretty

    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        indent = None
        if self.pretty:
            indent = 4

        return json.dumps(
            extracted_data,
            sort_keys=True,
            default=str,
            ensure_ascii=False,
            indent=indent,
        )
