from Router.router import Router
from Downloader.download import Downloader, PageDownloadException
from OutStreamer.outstreamer import OutStreamer
from utils import DomainRecord, PipeMetadata, metadata_logger


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
            extractor = self.router.route(
                metadata.domain_record.url, metadata.domain_record.timestamp, metadata
            )
            output = extractor.extract(downloaded_article, metadata)
            if output is None:
                metadata_logger.warn(
                    f"No output from {extractor.__class__}",
                    extra={"domain_record": metadata.domain_record},
                )
                return None
            path = await self.oustreamer.stream(output, metadata)
            metadata_logger.info(
                "Processed article", extra={"domain_record": metadata.domain_record}
            )

        except (ValueError, PageDownloadException) as e:
            metadata_logger.error(
                f"{e}", extra={"domain_record": metadata.domain_record}
            )
            return None
        return path
