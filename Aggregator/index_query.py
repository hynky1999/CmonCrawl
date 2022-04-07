from __future__ import annotations

from typing import Iterable, List, Optional, Tuple, Dict

import asyncio
import aiohttp
from aiohttp import ClientSession

class DomainIndexer:
    def __init__(self, domain_list: List[str]) -> None:
        self.domain_list = domain_list
        self.CDX_SERVER_URL = 'http://index.commoncrawl.org/'


    async def __aenter__(self) -> DomainIndexer:
        self.client_session: ClientSession = ClientSession()
        await self.client_session.__aenter__()
        return self

    # def __init_CC_server__(self) -> None:


    async def get_number_of_pages(self, url: str, CC_archive: str, page_size=None) -> int:
        params: Dict[str, str| bool] =  {'showNumPages': True, 'url': url, 'pageSize':page_size} }
        async with self.client_session.get(f'{self.CDX_SERVER_URL}/{CC_archive}', params=params) as response:
            r_json = await response.json()
            return r_json.get('numPages', 0)




    # def __aiter__() -> Iterable[DomainIndexer]:
    #     self.iter_list: list[str] = self.create_iter_list()
    #     return self

    # def __anext__(self) -> str:
    #     return self.iter_list.pop()

    # async def query_CC(self, CC_domain: str, query: str, paging: int) -> list[str]:
    #     await requests.get(f'https://{CC_domain}/api/v1/search?q={query}&page={paging}')


    async def __aexit__(self, exc_type, exc_val, exc_tb) -> DomainIndexer:
        await self.client_session.__aexit__(exc_type, exc_val, exc_tb)
        return self


    