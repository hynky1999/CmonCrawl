from typing import Any, AsyncContextManager, Optional
from cmoncrawl.common.types import DomainRecord


class ICC_Dao(AsyncContextManager):
    """
    ICC_Dao is a base class representing a Data Access Object for interacting with a data source.
    It provides methods for fetching data and managing the connection.

    Methods:
        fetch(domain_record): Fetches data for a given domain record.

    """

    async def fetch(self, domain_record: DomainRecord) -> bytes:
        raise NotImplementedError

    async def __aenter__(self) -> "ICC_Dao":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        pass


class DownloadError(Exception):
    def __init__(self, reason: str, status: Optional[int]):
        self.reason = reason
        self.status = status
