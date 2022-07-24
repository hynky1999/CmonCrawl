import asyncio
import json
from math import log
from pathlib import Path
import random
from typing import Any, Dict, List
from xmlrpc.client import Boolean
from OutStreamer.outstreamer import OutStreamer
from processor_utils import PipeMetadata, all_purpose_logger, metadata_logger
from aiofiles import open as asyncOpen

import os


class OutStreamerFileDefault(OutStreamer):
    def __init__(
        self,
        origin: Path,
        extension: str = ".out",
        directory_prefix: str = "directory_",
        max_directory_size: int = 500,
        order_num: bool = True,
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
        self.order_num = order_num
        self.size_padding = int(log(self.max_directory_size, 10)) + 1

    def __get_folder_path(self) -> Path:
        return self.origin / Path(f"{self.diretory_prefix}{len(self.directories)-1}")

    def __get_new_folder_path(self) -> Path:
        return self.origin / Path(f"{self.diretory_prefix}{len(self.directories)}")

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
            all_purpose_logger.debug(f"Removing {self.origin}")
            os.rmdir(self.origin)
        except OSError as e:
            all_purpose_logger.error(f"{e}")

    def get_file_name(self, metadata: PipeMetadata):
        name = metadata.name
        if name is None:
            name = f"{metadata.url_parsed.hostname}"
        name = f"{name}{self.extension}"
        order_num = None
        if self.order_num:
            order_num = f"{self.directory_size:0{self.size_padding}}"

        name_parts_list = [x for x in [order_num, name] if x is not None]
        return "_".join(name_parts_list)

    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        output = "\r\n\r\n".join(
            [f"{key}: \r\n {value}" for key, value in extracted_data.items()]
        )
        return output

    async def stream(
        self, extracted_data: Dict[Any, Any], metadata: PipeMetadata
    ) -> Path:
        while self.directory_size >= self.max_directory_size:
            all_purpose_logger.debug(
                f"Reached capacity of folder {self.__get_folder_path()} {self.directory_size}/{self.max_directory_size}"
            )
            self.__create_new_folder(self.__get_new_folder_path())
        # Preemptive so we dont' have to lock
        self.directory_size += 1
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
            async with asyncOpen(file_path, "w") as f:
                out = self.metadata_to_string(extracted_data)
                await f.write(out)
        except OSError as e:
            metadata_logger.error(
                f"{e}\n retrying {retries}/{self.max_retries}",
                extra={"domain_record": metadata.domain_record},
            )
            await asyncio.sleep(random.randint(1, 2))
            return await self.__stream(file_path, extracted_data, metadata, retries + 1)
        return file_path


class OutStreamerFileJSON(OutStreamerFileDefault):
    def __init__(
        self,
        origin: Path,
        extension: str = ".json",
        pretty: Boolean = False,
        **kwargs: Any,
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


class OutStreamerFileHTMLContent(OutStreamerFileDefault):
    def __init__(
        self,
        origin: Path,
        extension: str = ".html",
        **kwargs: Dict[Any, Any],
    ):
        super().__init__(origin, extension, **kwargs)

    def metadata_to_string(self, extracted_data: Dict[Any, Any]) -> str:
        return extracted_data["html"]
