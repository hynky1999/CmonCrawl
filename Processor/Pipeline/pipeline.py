from typing import Any


class ProcessorPipeline:
    def __init__(self, router, downloader, processor, outputer):
        self.router = router
        self.downloader = downloader
        self.processor = processor
        self.outputer = outputer

    def process_url(url: str):
        params: Dict[str, Any] = {}
        self.router.route(url)
        process_cls = params["processor"]
        process_cls.process(l)

