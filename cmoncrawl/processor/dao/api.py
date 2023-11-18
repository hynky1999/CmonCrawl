from cmoncrawl.common.types import DomainRecord
from cmoncrawl.processor.dao.base import DownloadError, ICC_Dao
from aiohttp import (
    ClientError,
    ClientSession,
    ContentTypeError,
    ServerConnectionError,
)
from typing import Any

BASE_URL = "https://data.commoncrawl.org/"
ALLOWED_ERR_FOR_RETRIES = [500, 502, 503, 504]


class CCAPIGatewayDAO(ICC_Dao):
    """
    This class represents a DAO (Data Access Object) for interacting with the Common Crawl API Gateway.
    It provides methods for opening and closing a connection, fetching data for a given domain record,
    and handling errors related to downloading data.

    Args:
        base_url (str): The base URL of the Common Crawl API Gateway. Defaults to "https://data.commoncrawl.org/".

    Methods:
        aopen: Asynchronously opens a connection to the API Gateway.
        __aenter__: Asynchronously enters a context manager and returns the DAO instance.
        aclose: Asynchronously closes the connection to the API Gateway.
        __aexit__: Asynchronously exits a context manager.
        fetch: Asynchronously fetches data for a given domain record.

    Example usage:
        dao = CCAPIGatewayDAO()
        async with dao:
            data = await dao.fetch(domain_record)
    """

    def __init__(self, base_url: str = BASE_URL):
        self.BASE_URL = base_url

    async def aopen(self) -> "CCAPIGatewayDAO":
        self.client: ClientSession = ClientSession()
        await self.client.__aenter__()
        return self

    async def __aenter__(self) -> "CCAPIGatewayDAO":
        return await self.aopen()

    async def aclose(self) -> None:
        await self.client.close()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def fetch(self, domain_record: DomainRecord) -> bytes:
        headers = {
            "Range": "bytes={}-{}".format(
                domain_record.offset,
                domain_record.offset + domain_record.length - 1,
            )
        }
        url = f"{self.BASE_URL}{domain_record.filename}"

        try:
            async with self.client.get(url, headers=headers) as response:
                if not response.ok:
                    reason: str = (
                        str(response.reason) if response.reason else "Unknown"
                    )
                    if response.status in ALLOWED_ERR_FOR_RETRIES:
                        raise DownloadError(reason, response.status)

                    # We don't want to retry
                    raise ValueError(
                        f"Failed to download {url} with status {response.status} and reason {reason}"
                    )

                # will be unzipped, we cannot use the stream since warcio doesn't support async
                response = await response.content.read()
        except (
            ClientError,
            TimeoutError,
            ServerConnectionError,
            ContentTypeError,
        ) as e:
            raise DownloadError(str(e), None)

        return response
