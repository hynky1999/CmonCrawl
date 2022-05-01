from aiohttp import ClientSession
from typing import Any, Dict
from Aggregator.errors import PageResponseError


async def download_url(client: ClientSession, params: Dict[str, Any]) -> str:
    url = params["url"]
    fetch_params = params.get("url_params", {})

    async with client.get(url, params=fetch_params) as response:
        if not response.ok:
            raise PageResponseError(url, response.reason, response.status)
        html = await response.text()
        return html
