from pathlib import Path
from typing import List
from Router.router import Router
from Downloader.download import Downloader
from OutStreamer.outstreamer import OutStreamer
from processor_utils import DomainRecord, PipeMetadata, metadata_logger
from warcio.exceptions import ArchiveLoadFailed


class ProcessorPipeline:
    def __init__(
        self, router: Router, downloader: Downloader, outstreamer: OutStreamer
    ):
        self.router = router
        self.downloader = downloader
        self.oustreamer = outstreamer

    async def process_domain_record(self, domain_record: DomainRecord):
        paths: List[Path] = []
        downloaded_articles = []
        try:
            downloaded_articles = await self.downloader.download(domain_record)
        except (ArchiveLoadFailed) as e:
            metadata_logger.error(f"{e}", extra={"domain_record": domain_record})

        try:
            for (downloaded_article, metadata) in downloaded_articles:
                extractor = self.router.route(
                    metadata.domain_record.url,
                    metadata.domain_record.timestamp,
                    metadata,
                )
                output = extractor.extract(downloaded_article, metadata)
                if output is None:
                    metadata_logger.warn(
                        f"No output from {extractor.__class__}",
                        extra={"domain_record": metadata.domain_record},
                    )
                    continue
                paths.append(await self.oustreamer.stream(output, metadata))
                metadata_logger.info(
                    "Processed article", extra={"domain_record": metadata.domain_record}
                )
        # Not catching IOError because some other processor could process it -> nack
        except ValueError as e:
            metadata_logger.error(
                str(e),
                extra={"domain_record": domain_record},
            )
        return paths
