from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse
from dataclasses import dataclass, field
from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime

from pydantic import BaseModel


def parse_timestamp(v: Optional[Any]) -> Optional[datetime]:
    if v is None:
        return None

    if isinstance(v, datetime):
        return v

    if isinstance(v, str):
        return datetime.fromisoformat(v)

    raise ValueError(f"Invalid timestamp: {v}")


class DomainRecord(BaseModel):
    """
    Domain record.
    """

    filename: str
    url: str | None
    offset: int
    length: int
    digest: str | None = None
    encoding: str | None = None
    timestamp: Optional[datetime] = Field(None)

    @validator("timestamp", pre=True)
    def parse_timestamp(cls, v: Optional[str]) -> Optional[datetime]:
        return parse_timestamp(v)


@dataclass
class PipeMetadata:
    """
    Metadata for a pipe.

    Attributes:
    domain_record: DomainRecord
        An instance of the DomainRecord class representing associated domain record,
        eg. pointer to the WARC file.

    article_data: Dict[Any, Any] = field(default_factory=dict)
        A dictionary storing article data with keys and values of any type.
        Those are the data extracted using Extractors.

    warc_header: Dict[str, Any] = field(default_factory=dict)
        A dictionary storing the WARC header metadata.

    http_header: Dict[str, Any] = field(default_factory=dict)
        A dictionary storing the HTTP header information.

    rec_type: str | None = None
        A string or None representing the type of record.

    encoding: str = "latin-1"
        A string representing the character encoding used for the record. The default value is "latin-1".

    name: str | None = None
        A string or None representing the name associated with the record.
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


class ExtractorConfig(BaseModel):
    """
    Configuration for extractor.
    """

    name: str
    since: Optional[datetime] = Field(None)
    to: Optional[datetime] = Field(None)

    @validator("since", "to", pre=True)
    def parse_timestamp(cls, v: Optional[str]) -> Optional[datetime]:
        return parse_timestamp(v)


class RoutesConfig(BaseModel):
    """
    Configuration for extractors.
    """

    regexes: List[str] = []
    extractors: List[ExtractorConfig] = []


class ExtractConfig(BaseModel):
    """
    Configuration for run.
    """

    extractors_path: Path
    routes: List[RoutesConfig]


class MatchType(Enum):
    """
    Match type for cdx server.
    See https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md#url-match-scope

    Example:
    Query: example.com/abc

    Macthes:
    EXACT: (www\.)?example.com/abc
    PREFIX: (www\.)?example.com/abc(/.*)?
    HOST: (www\.)?example.com(/.*)?
    DOMAIN: (.*\.)?example.com(/.*)?
    """

    EXACT = "exact"
    PREFIX = "prefix"
    HOST = "host"
    DOMAIN = "domain"

    def __str__(self):
        return self.value
