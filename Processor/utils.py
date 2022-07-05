from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict
from urllib.parse import urlparse

DEFAULT_ENCODING = "latin-1"


@dataclass
class DomainRecord:
    filename: str
    url: str
    offset: int
    length: int
    digest: str | None = None
    encoding: str = DEFAULT_ENCODING
    timestamp: datetime = datetime.now()


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
