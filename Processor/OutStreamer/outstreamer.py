from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict
from Processor.Downloader.warc import PipeMetadata


class OutStreamer(ABC):
    def __init__(self):
        pass

    @abstractmethod
    async def stream(self, extracted_data: Dict[Any, Any],  metadata: PipeMetadata) -> Path:
        raise NotImplementedError()

    @abstractmethod
    async def clean_up(self) -> None:
        raise NotImplementedError()

