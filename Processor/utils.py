from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict
from urllib.parse import urlparse


@dataclass
class DomainRecord:
    filename: str
    url: str
    offset: int
    length: int
    digest: str | None = None
    encoding: str | None = None
    timestamp: datetime = datetime.now()


@dataclass
class PipeMetadata:
    """
    Metadata for a pipe.
    """

    domain_record: DomainRecord
    article_data: Dict[Any, Any] = field(default_factory=dict)
    warc_header: Dict[str, Any] = field(default_factory=dict)
    http_header: Dict[str, Any] = field(default_factory=dict)
    encoding: str = "latin-1"
    name: str | None = None

    def __post_init__(self):
        self.url_parsed = urlparse(self.domain_record.url)
