from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse
from dataclasses import dataclass, field
from marshmallow import fields

from dataclasses_json import dataclass_json, config


@dataclass_json
@dataclass
class DomainRecord:
    """
    Domain record.
    """

    filename: str
    url: str | None
    offset: int
    length: int
    digest: str | None = None
    encoding: str | None = None
    timestamp: datetime | None = field(
        metadata=config(mm_field=fields.DateTime(format="iso")), default=None
    )


@dataclass
class PipeMetadata:
    """
    Metadata for a pipe.
    """

    domain_record: DomainRecord
    article_data: Dict[Any, Any] = field(default_factory=dict)
    warc_header: Dict[str, Any] = field(default_factory=dict)
    http_header: Dict[str, Any] = field(default_factory=dict)
    rec_type: str | None = None
    encoding: str = "latin-1"
    name: str | None = None

    def __post_init__(self):
        self.url_parsed = urlparse(self.domain_record.url)


@dataclass
class RetrieveResponse:
    """
    Response from retrieve.
    """

    status: int
    content: Any
    reason: None | str


@dataclass
class DomainCrawl:
    """
    Domain crawl.
    """

    domain: str = ""
    cdx_server: str = ""
    page: int = 0


# ===============================================================================
# Extractor config


@dataclass_json
@dataclass
class ExtractorConfig:
    """
    Configuration for extractor.
    """

    name: str
    since: datetime | None = field(
        metadata=config(mm_field=fields.DateTime(format="iso")), default=None
    )
    to: datetime | None = field(
        metadata=config(mm_field=fields.DateTime(format="iso")), default=None
    )


@dataclass_json
@dataclass
class RoutesConfig:
    """
    Configuration for extractors.
    """

    regexes: list[str] = field(default_factory=list)
    extractors: list[ExtractorConfig] = field(default_factory=list)


@dataclass_json
@dataclass
class ExtractConfig:
    """
    Configuration for run.
    """

    extractors_path: Path
    routes: List[RoutesConfig]


class MatchType(Enum):
    """
    Match type for cdx server.
    """

    EXACT = "exact"
    PREFIX = "prefix"
    HOST = "host"
    DOMAIN = "domain"
