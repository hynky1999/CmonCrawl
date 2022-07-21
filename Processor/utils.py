from dataclasses import dataclass, field
from datetime import datetime
import logging
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


metadata_logger = logging.getLogger("metadata_logger")
metadata_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(filename)s:%(lineno)d : %(levelname)s - %(message)s -> ADD_INFO: %(domain_record)s"
    )
)
metadata_logger.addHandler(handler)

all_purpose_logger = logging.getLogger("all_purpose_logger")
metadata_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
    )
)
all_purpose_logger.addHandler(handler)
