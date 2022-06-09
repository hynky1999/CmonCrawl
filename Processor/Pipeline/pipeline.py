from typing import Any, Dict
from Aggregator.index_query import DomainRecord


class ProcessorPipeline:
    def __init__(self, router, downloader, processor, outstreamer):
        self.router = router
        self.downloader = downloader
        self.processor = processor
        self.oustreamer = outstreamer

    def process_domain_record(self, domain_record: DomainRecord):
        downloaded_article = self.downloader.download(domain_record)
        processor_cls = self.router.route(downloaded_article)
        output = process_cls.process(downloaded_article)
        path = self.outstreamer.outstream(output)
        return path

