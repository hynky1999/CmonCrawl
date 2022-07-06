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
        try:
            downloaded_article = await self.downloader.download(domain_record, metadata)
            extractor = self.router.route(metadata.domain_record.url)
        except ValueError as e:
            logging.error(f"{domain_record.url}: {e}")
            return None

        output = extractor.extract(downloaded_article, metadata)
        if output is None:
            return None
        path = await self.oustreamer.stream(output, metadata)
        return path
