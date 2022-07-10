import logging
from Extractor.extractor import BaseExtractor
from utils import PipeMetadata


class Extractor(BaseExtractor):
    def extract_no_preprocess(self, response: str, metadata: PipeMetadata):
        metadata.name = metadata.domain_record.url.replace("/", "_")

        return {"html": response}

    def filter(self, response: str, metadata: PipeMetadata):
        if metadata.http_header.get("http_response_code", 0) != 200:
            logging.warning(
                f"Status: {metadata.http_header.get('http_response_code', 0)}"
            )
            return False
        return True
