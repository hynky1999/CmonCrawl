from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import urlparse
from Aggregator.index_query import DomainRecord


@dataclass
class PipeMetadata:
    """
    Metadata for a pipe.
    """

    domain_record: DomainRecord
    article_data: Dict[Any, Any] | None = None
    warc_header: Dict[str, Any] | None = None
    http_header: Dict[str, Any] | None = None
    name: str | None = None

    def __post_init__(self):
        self.url_parsed = urlparse(self.domain_record.url)
