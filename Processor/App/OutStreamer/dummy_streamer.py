from pathlib import Path
from typing import Any, Dict, List, Tuple
from Processor.App.OutStreamer.outstreamer import OutStreamer
from Processor.App.processor_utils import PipeMetadata


class DummyStreamer(OutStreamer):
    def __init__(self):
        self.data: Dict[str, Dict[Any, Any]] = {}

    async def stream(self, extracted_data: Dict[Any, Any], metadata: PipeMetadata):
        self.data[metadata.domain_record.filename] = extracted_data
        return Path("/")

    async def clean_up(self):
        pass
