from pathlib import Path
from typing import List
from Processor.Downloader.warc import PipeMetadata
from Processor.OutStreamer.outstreamer import OutStreamer
from aiofiles import open as asyncOpen

import os


class OutStreamerFile(OutStreamer):
    def __init__(
        self,
        origin: Path,
        extension: str = ".html",
        directory_prefix: str = "directory_",
        max_directory_size: int = 1000,
    ):
        self.directory_num = 0
        self.directory_size = 0
        self.origin = origin
        self.max_directory_size = max_directory_size
        self.diretory_prefix = directory_prefix
        self.directories: List[Path] = []
        self.__create_new_folder(self.__get_folder_path())
        self.extension = extension

    def __get_folder_path(self) -> Path:
        return self.origin / Path(f"{self.diretory_prefix}{self.directory_num}")

    def __create_new_folder(self, folder_path: Path):
        os.makedirs(folder_path, exist_ok=True)
        self.directory_size = len(os.listdir(folder_path))
        self.directories.append(folder_path)

    async def clean_up(self) -> None:
        for directory in self.directories:
            for root, dirs, files in os.walk(directory, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

    async def stream(self, response: str, metadata: PipeMetadata) -> Path:
        if self.directory_size >= self.max_directory_size:
            print(f"Reached capacity of folder {self.__get_folder_path()}")
            self.directory_num += 1
            self.__create_new_folder(self.__get_folder_path())

        file_path = Path(self.__get_folder_path()) / Path(
            f"{hash(response)}_{self.directory_size}{self.extension}"
        )
        try:
            self.directory_size += 1
            async with asyncOpen(file_path, "w") as f:
                await f.write(response)

        except OSError:
            # TODO: better error handling
            self.directory_size -= 1
        return file_path

