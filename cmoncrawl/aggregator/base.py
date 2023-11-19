import abc
from typing import AsyncContextManager, AsyncIterable

from cmoncrawl.common.types import DomainRecord


class IAggregator(AsyncContextManager, AsyncIterable[DomainRecord], abc.ABC):
    """
    Base interface for aggregators
    """
