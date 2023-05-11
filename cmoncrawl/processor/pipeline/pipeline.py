from pathlib import Path
from typing import Any, Dict, List
from cmoncrawl.processor.pipeline.downloader import IDownloader
from cmoncrawl.processor.pipeline.streamer import IStreamer
from cmoncrawl.processor.pipeline.router import IRouter
from cmoncrawl.common.types import DomainRecord
from cmoncrawl.common.loggers import metadata_logger
from warcio.exceptions import ArchiveLoadFailed


class ProcessorPipeline:
    def __init__(
        self, router: IRouter, downloader: IDownloader, outstreamer: IStreamer
    ):
        self.router = router
        self.downloader = downloader
        self.oustreamer = outstreamer

    async def process_domain_record(
        self, domain_record: DomainRecord, additional_info: Dict[str, Any]
    ):
        paths: List[Path] = []
        downloaded_articles = []
        try:
            downloaded_articles = await self.downloader.download(domain_record)
        except (ArchiveLoadFailed) as e:
            metadata_logger.error(f"{e}", extra={"domain_record": domain_record})

        for (downloaded_article, metadata) in downloaded_articles:
            extractor = self.router.route(
                metadata.domain_record.url,
                metadata.domain_record.timestamp,
                metadata,
            )
            output = extractor.extract(downloaded_article, metadata)
            if output is None:
                metadata_logger.warn(
                    f"Extractor {extractor.__class__.__name__} returned None for {metadata.domain_record.url}",
                    extra={"domain_record": metadata.domain_record},
                )
                continue

            if "additional_info" not in output:
                output["additional_info"] = additional_info

            paths.append(await self.oustreamer.stream(output, metadata))
        return paths
