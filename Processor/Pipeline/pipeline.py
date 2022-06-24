from Aggregator.index_query import DomainRecord
from Processor.Router.router import Router
from Processor.Downloader.download import Downloader
from Processor.OutStreamer.outstreamer import OutStreamer
from Processor.utils import PipeMetadata


class ProcessorPipeline:
    def __init__(
        self, router: Router, downloader: Downloader, outstreamer: OutStreamer
    ):
        self.router = router
        self.downloader = downloader
        self.oustreamer = outstreamer

    async def process_domain_record(self, domain_record: DomainRecord):
        try:
            metadata = PipeMetadata(domain_record=domain_record)
            downloaded_article = await self.downloader.download(domain_record, metadata)
            # This will not be None
            extractor_cls = self.router.route(metadata.domain_record.url)
            extractor = extractor_cls()
            output: str = extractor.extract(downloaded_article, metadata)
            if output is None:
                return None
            path = await self.oustreamer.stream(output, metadata)
            print("Successfully downloaded article from {}".format(domain_record.url))
            return path
        except Exception as e:
            print(e)
            return None

