from dataclasses import dataclass
from typing import Any, Dict
from Aggregator.index_query import DomainRecord


@dataclass
class PipeMetadata:
    """
    Metadata for a pipe.
    """

    domain_record: DomainRecord
    article_data: Any = None
    warc_header: Dict[str, Any] | None = None
    http_header: Dict[str, Any] | None = None
