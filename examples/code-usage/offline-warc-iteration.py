import asyncio
from pathlib import Path
from typing import Any, Dict

from bs4 import BeautifulSoup

from cmoncrawl.common.loggers import all_purpose_logger, metadata_logger
from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.processor.pipeline.downloader import WarcIterator
from cmoncrawl.processor.pipeline.extractor import BaseExtractor
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
from cmoncrawl.processor.pipeline.router import Router
from cmoncrawl.processor.pipeline.streamer import IStreamer

# Make sure you have cmoncrawl >= 1.0.3 installed

# Here we define our class for custom extractor, since we inherit from BaseExtractor we need to define the extract_soup method,
# which defined what we want to extract. It take the parsed soup and additional metadata (http parameters, timestamp, url, etc) and you need
# to return a dictionary[str, Any] with the data you want to extract, if you fail to extract anything you required, return None.

# Other than that you can also define filter_raw and filter_soup methods, which take the raw response and parsed soup respectively,
# and you need to return a boolean, True if you want to extract the data, False if you don't want to extract the data.

# There are few useful methods that can help with filtering in cmoncrawl.processor.extraction.filters
# There are some useful methods that can help you with processing the data in cmoncrawl.processor.extraction.utils,
# such as check_required, in which you defined the required fields and call it with the data you extracted, it will return True if all required fields are present.


class MyExtractor(BaseExtractor):
    def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
        # Here we simply extract the title of the page
        title_text = soup.title.text if soup.title else None

        # And return it in a dictionary
        return {
            "title": title_text,
            "url": metadata.domain_record.url,
            "timestamp": metadata.domain_record.timestamp,
        }

    def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
        # Here we check that the warc record is of type http response,
        # you don't want to extract data from http request or metadata
        if metadata.rec_type != "response":
            return False
        return True


# Here we define our streamer, which is responsible for writing the extracted data to a file, database, etc.
# In order to define your own streamer, you need to inherit from IStreamer and define the stream method, which takes the extracted data and metadata,
# and return string identifier for the data you extracted, if you don't want to write the data or failed to write the data, return None.
# Additionaly you need to define the clean_up method, which is called after the pipeline is done, and is responsible for cleaning up the streamer.
class MyStreamer(IStreamer):
    async def stream(
        self, extracted_data: Dict[Any, Any], metadata: PipeMetadata
    ) -> str | None:
        # call your db, write to file, etc.
        print(f"Extracted data: {extracted_data}")
        return metadata.domain_record.url

    async def clean_up(self) -> None:
        pass


metadata_logger.setLevel("WARN")
all_purpose_logger.setLevel("WARN")
# Define the warc file you want to process
file = Path("CC-MAIN-20220123045449-20220123075449-00355.warc.gz")

# Here we define the router, which is responsible for routing the warc record to the correct extractor.
router = Router()
# We add our custom extractor and name it "test"
router.load_extractor("test", MyExtractor())
# We route all warc records to our custom extractor
router.register_route("test", ".*", None, None)
streamer = MyStreamer()
# Here we create the Downloader (WarcIterator) and the ProcessorPipeline, and start processing the warc file.
# We open the WarcIterator as ContextManager (we need to have the file open for the whole duration of the processing) thus we use the with statement.
with WarcIterator(file, show_progress=True) as warc:
    pipe = ProcessorPipeline(downloader=warc, router=router, outstreamer=streamer)
    result = asyncio.run(pipe.process_domain_record(None, {}))
    print(result)
