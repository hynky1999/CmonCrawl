import logging
from Router.router import Router
from Downloader.download import Downloader
from OutStreamer.outstreamer import OutStreamer
from utils import DomainRecord, PipeMetadata


class ProcessorPipeline:
    def __init__(
        self, router: Router, downloader: Downloader, outstreamer: OutStreamer
    ):
        self.router = router
        self.downloader = downloader
        self.oustreamer = outstreamer

    async def process_domain_record(self, domain_record: DomainRecord):
        metadata = PipeMetadata(domain_record=domain_record)
        downloaded_article = await self.downloader.download(domain_record, metadata)
        # This will not be None
        extractor = self.router.route(metadata.domain_record.url)
        output = extractor.extract(downloaded_article, metadata)
        if output is None:
            print(f" Failed to download {metadata.domain_record.url}")
            return None
        path = await self.oustreamer.stream(output, metadata)

        logging.debug(
            f"Successfully downloaded article from {domain_record.url} at {path}"
        )
        return path
